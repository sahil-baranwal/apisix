--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local require = require
local ipairs   = ipairs
local pcall   = pcall
local exiting      = ngx.worker.exiting
local pairs    = pairs
local tostring = tostring
local core = require("apisix.core")
local config_local   = require("apisix.core.config_local")
local resource = require("apisix.resource")
local upstream_utils = require("apisix.utils.upstream")
local healthcheck
local tab_clone = core.table.clone
local timer_every = ngx.timer.every
local jp = require("jsonpath")
local config_util = require("apisix.core.config_util")

local _M = {}
local working_pool = {}     -- resource_path -> {version, checker, checks,
                            --                   pass_host, upstream_host, targets_map}
local waiting_pool = {}      -- resource_path -> resource_ver

local DELAYED_CLEAR_TIMEOUT = 10
local healthcheck_shdict_name = "upstream-healthcheck"


local function get_healthchecker_name(value)
    return "upstream#" .. (value.resource_key or value.upstream.resource_key)
end
_M.get_healthchecker_name = get_healthchecker_name


local function create_checker(up_conf)
    if not up_conf.checks then
        return nil
    end
    local local_conf = config_local.local_conf()
    if local_conf and local_conf.apisix and local_conf.apisix.disable_upstream_healthcheck then
        core.log.info("healthchecker won't be created: disabled upstream healthcheck")
        return nil
    end
    core.log.info("creating healthchecker for upstream: ", up_conf.resource_key)
    if not healthcheck then
        healthcheck = require("resty.healthcheck")
    end

    local checker, err = healthcheck.new({
        name = get_healthchecker_name(up_conf),
        shm_name = healthcheck_shdict_name,
        checks = up_conf.checks,
        events_module = "resty.events",
    })

    if not checker then
        core.log.error("failed to create healthcheck: ", err)
        return nil
    end

    -- Add target nodes
    local targets_map = {}
    local active = up_conf.checks and up_conf.checks.active
    local host = active and active.host
    local port = active and active.port
    local up_hdr = up_conf.pass_host == "rewrite" and up_conf.upstream_host
    local use_node_hdr = up_conf.pass_host == "node" or nil

    for _, node in ipairs(up_conf.nodes) do
        local host_hdr = up_hdr or (use_node_hdr and node.domain)
        local t_port = port or node.port
        local hostname = host or node.host
        local ok, err = checker:add_target(node.host, t_port, hostname, true, host_hdr)
        if not ok then
            core.log.error("failed to add healthcheck target: ", node.host, ":",
                          t_port, " err: ", err)
        end
        targets_map[node.host .. ":" .. tostring(t_port) .. ":" .. tostring(hostname)] = {
            ip = node.host,
            port = t_port,
            hostname = hostname,
        }
    end

    return checker, targets_map
end


-- Reconcile a live checker's targets with the current upstream nodes without
-- destroying the checker. Existing targets keep their health status because
-- add_target is a no-op for targets that already exist; only genuinely new
-- nodes are added (as healthy) and removed nodes are deleted from the checker.
local function update_checker_targets(checker, up_conf, old_targets_map)
    local new_targets_map = {}
    local active = up_conf.checks and up_conf.checks.active
    local host = active and active.host
    local port = active and active.port
    local up_hdr = up_conf.pass_host == "rewrite" and up_conf.upstream_host
    local use_node_hdr = up_conf.pass_host == "node" or nil

    for _, node in ipairs(up_conf.nodes) do
        local host_hdr = up_hdr or (use_node_hdr and node.domain)
        local t_port = port or node.port
        local hostname = host or node.host
        new_targets_map[node.host .. ":" .. tostring(t_port) .. ":" .. tostring(hostname)] = {
            ip = node.host,
            port = t_port,
            hostname = hostname,
        }
        local ok, err = checker:add_target(node.host, t_port, hostname, true, host_hdr)
        if not ok then
            core.log.error("failed to add healthcheck target: ", node.host, ":",
                          t_port, " err: ", err)
        end
    end

    for key, target in pairs(old_targets_map) do
        if not new_targets_map[key] then
            local ok, err = checker:remove_target(target.ip, target.port, target.hostname)
            if not ok then
                core.log.error("failed to remove healthcheck target: ", target.ip, ":",
                              target.port, " err: ", err)
            else
                core.log.info("removed healthcheck target: ", target.ip, ":", target.port)
            end
        end
    end

    return new_targets_map
end


-- A change is only structural (requires rebuilding the checker) when the
-- checks block itself or the host-header shaping changes. Node additions and
-- removals are handled incrementally so health status is preserved.
local function checks_config_equal(item, up_conf)
    return core.table.deep_eq(item.checks, up_conf.checks)
       and item.pass_host == up_conf.pass_host
       and item.upstream_host == up_conf.upstream_host
end


local function add_working_pool(resource_path, resource_ver, checker, up_conf, targets_map)
    working_pool[resource_path] = {
        version = resource_ver,
        checker = checker,
        checks = up_conf.checks,
        pass_host = up_conf.pass_host,
        upstream_host = up_conf.upstream_host,
        targets_map = targets_map,
    }
end


function _M.fetch_checker(resource_path, resource_ver)
    local working_item = working_pool[resource_path]
    if working_item and working_item.version == resource_ver then
        return working_item.checker
    end

    if waiting_pool[resource_path] == resource_ver then
        return nil
    end

    -- Add to waiting pool with version
    core.log.info("adding ", resource_path, " to waiting pool with version: ", resource_ver)
    waiting_pool[resource_path] = resource_ver
    return nil
end


function _M.fetch_node_status(checker, ip, port, hostname)
    -- check if the checker is valid
    if not checker or checker.dead then
        return true
    end

    return checker:get_target_status(ip, port, hostname)
end


local function get_plugin_name(path)
    -- Extract JSON path (after '#') or use full path
    local json_path = path:match("#(.+)$") or path
    -- Match plugin name in the JSON path segment
    return json_path:match("^plugins%['([^']+)'%]")
        or json_path:match('^plugins%["([^"]+)"%]')
        or json_path:match("^plugins%.([^%.]+)")
end


-- Resolve the upstream config (and its resource_key) from a fetched resource
-- config, transparently handling plugin-provided dynamic upstreams.
-- Returns nil when the resource no longer exists or has no value.
local function resolve_upstream(resource_path, res_conf)
    if not (res_conf and res_conf.value) then
        return nil
    end

    local upstream
    local plugin_name = get_plugin_name(resource_path)
    if plugin_name and plugin_name ~= "" then
        local _, sub_path = config_util.parse_path(resource_path)
        local json_path = "$." .. sub_path
        --- the users of the API pass the jsonpath(in resourcepath) to
        --- upstream_constructor_config which is passed to the
        --- callback construct_upstream to create an upstream dynamically
        local upstream_constructor_config = jp.value(res_conf.value, json_path)
        local plugin = require("apisix.plugins." .. plugin_name)
        upstream = plugin.construct_upstream(upstream_constructor_config)
        upstream.resource_key = resource_path
    else
        upstream = res_conf.value.upstream or res_conf.value
    end

    return upstream
end


-- Reconcile a single resource's checker against the latest config. Safe to call
-- from a single timer that sweeps both the waiting and working pools.
--
-- Always reconcile against the latest known version (not the version requested
-- when the resource was queued); this avoids a race where the queued version is
-- already stale and the checker would otherwise never be (re)created.
--
-- create_checker() yields (it broadcasts target events over a cosocket), so the
-- checker is built BEFORE the old one is torn down and swapped in atomically.
-- This keeps working_pool[resource_path] pointing at a live checker across the
-- yield, so a concurrent fetch_checker() (request path) never observes a checker
-- that has already been stopped, and a failed build leaves the old one intact.
local function reconcile_resource(resource_path)
    local res_conf = resource.fetch_latest_conf(resource_path)
    local upstream = resolve_upstream(resource_path, res_conf)

    local item = working_pool[resource_path]

    if not upstream then
        -- resource doesn't exist anymore, destroy the checker if we have one
        if item then
            working_pool[resource_path] = nil
            item.checker.dead = true
            item.checker:delayed_clear(DELAYED_CLEAR_TIMEOUT)
            item.checker:stop()
            core.log.info("released checker: ", tostring(item.checker), " for resource: ",
                        resource_path, " and version : ", item.version)
        end
        return
    end

    local new_version = upstream_utils.version(res_conf.modifiedIndex,
                                               upstream._nodes_ver)
    core.log.info("reconciling resource: ", resource_path,
                " current version: ", new_version,
                " item version: ", item and item.version or "nil")

    if item and item.version == new_version then
        return
    end

    if item and checks_config_equal(item, upstream) then
        -- only nodes changed: reconcile targets on the live checker in place
        item.targets_map = update_checker_targets(item.checker, upstream,
                                                  item.targets_map)
        item.version = new_version
        return
    end

    -- structural change (or first creation): build new, swap, then retire old
    local checker, targets_map = create_checker(upstream)
    if not checker then
        -- build failed; keep the old checker (if any) running
        return
    end
    local old = item
    add_working_pool(resource_path, new_version, checker, upstream, targets_map)
    if old then
        old.checker:delayed_clear(DELAYED_CLEAR_TIMEOUT)
        old.checker:stop()
    end
end


-- Single reconcile timer for both pools. Running both sweeps in one timer (with
-- a single re-entry guard) means the two sweeps can never interleave on the same
-- resource across create_checker()'s yield, which would otherwise let two timers
-- each build a checker and leak the loser.
local function timer_reconcile()
    if core.table.nkeys(waiting_pool) > 0 then
        local waiting_snapshot = tab_clone(waiting_pool)
        for resource_path in pairs(waiting_snapshot) do
            reconcile_resource(resource_path)
            waiting_pool[resource_path] = nil
        end
    end

    if core.table.nkeys(working_pool) > 0 then
        local working_snapshot = tab_clone(working_pool)
        for resource_path in pairs(working_snapshot) do
            reconcile_resource(resource_path)
        end
    end
end

function _M.init_worker()
    local timer_reconcile_running = false
    timer_every(1, function ()
        if exiting() then
            return
        end
        if timer_reconcile_running then
            core.log.warn("timer_reconcile is already running, skipping this iteration")
            return
        end
        timer_reconcile_running = true
        local ok, err = pcall(timer_reconcile)
        if not ok then
            core.log.error("failed to run timer_reconcile: ", err)
        end
        timer_reconcile_running = false
    end)
end

return _M
