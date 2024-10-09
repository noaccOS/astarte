#
# This file is part of Astarte.
#
# Copyright 2017 - 2025 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

defmodule Astarte.DataUpdaterPlant.DataUpdater.Impl do
  @moduledoc """
  This module implements the core logic of the DataUpdater process.
  """
  @behaviour Mississippi.Consumer.DataUpdater.Handler

  alias Astarte.Core.Device
  alias Astarte.DataUpdaterPlant.Config
  alias Astarte.DataUpdaterPlant.DataUpdater.Cache
  alias Astarte.DataUpdaterPlant.DataUpdater.Core
  alias Astarte.DataUpdaterPlant.DataUpdater.Queries
  alias Astarte.DataUpdaterPlant.DataUpdater.State
  alias Astarte.DataUpdaterPlant.TimeBasedActions
  alias Astarte.DataUpdaterPlant.TriggerPolicy.Queries, as: PolicyQueries
  alias Astarte.DataUpdaterPlant.TriggersHandler
  alias Astarte.DataUpdaterPlant.ValueMatchOperators
  alias Astarte.RPC.Protocol.DataUpdaterPlant.DeleteVolatileTrigger
  alias Astarte.RPC.Protocol.DataUpdaterPlant.InstallVolatileTrigger
  require Logger

  @msg_type_header "x_astarte_msg_type"
  @ip_header "x_astarte_remote_ip"
  @internal_path_header "x_astarte_internal_path"
  @interface_header "x_astarte_interface"
  @path_header "x_astarte_path"
  @control_path_header "x_astarte_control_path"

  @impl true
  def init(sharding_key) do
    # TODO change this, we want extended device IDs to fall in the same process
    {realm, device_id} = sharding_key

    state = %State{
      realm: realm,
      device_id: device_id,
      paths_cache: Cache.new(Config.paths_cache_size!())
    }

    encoded_device_id = Device.encode_device_id(device_id)
    Logger.metadata(realm: realm, device_id: encoded_device_id)
    Logger.info("Created device process.", tag: "device_process_created")

    device_status = Queries.get_device_status(state.realm, device_id)

    # TODO this could be a bang!
    {:ok, ttl} = Queries.get_datastream_maximum_storage_retention(state.realm)

    Map.merge(state, device_status)
    |> Map.put(:datastream_maximum_storage_retention, ttl)
  end

  @impl true
  def handle_message(payload, headers, _message_id, timestamp, state) do
    %{@msg_type_header => message_type} = headers

    case message_type do
      "connection" ->
        %{@ip_header => ip_address} = headers
        handle_connection(state, ip_address, timestamp)

      "disconnection" ->
        handle_disconnection(state, timestamp)

      "heartbeat" ->
        Core.HeartbeatHandler.handle_heartbeat(state, timestamp)

      "internal" ->
        %{@internal_path_header => internal_path} = headers
        handle_internal(state, internal_path, payload, timestamp)

      "introspection" ->
        handle_introspection(state, payload, timestamp)

      "data" ->
        %{@interface_header => interface, @path_header => path} = headers
        handle_data(state, interface, path, payload, timestamp)

      "control" ->
        %{@control_path_header => control_path} = headers
        handle_control(state, control_path, payload, timestamp)

      "capabilities" ->
        handle_capabilities(state, payload, timestamp)

      _ ->
        # Ack all messages for now
        {:ack, :ok, state}
    end
  end

  @impl true
  def handle_signal(signal, state) do
    case signal do
      {:handle_install_volatile_trigger, parent_id, trigger_id, simple_trigger, trigger_target} ->
        handle_install_volatile_trigger(state, parent_id, trigger_id, simple_trigger, trigger_target)

      {:handle_delete_volatile_trigger, trigger_id} ->
        handle_delete_volatile_trigger(state, trigger_id)

      :dump_state ->
        {state, state}

      {:start_device_deletion, timestamp} ->
        start_device_deletion(state, timestamp)

      _ ->
        {:ok, state}
    end
  end

  @impl true
  def terminate(_, state) do
    # All is ok for now
    {:ok, state}
  end

  def handle_deactivation(_state) do
    Logger.info("Deactivated device process.", tag: "device_process_deactivated")

    :ok
  end

  def handle_connection(%State{discard_messages: true} = state, _, _, _) do
    {:ack, :discard_message, state}
  end

  def handle_connection(state, ip_address_string, timestamp) do
    new_state = TimeBasedActions.execute_time_based_actions(state, timestamp)

    timestamp_ms = div(timestamp, 10_000)

    ip_address_result =
      ip_address_string
      |> to_charlist()
      |> :inet.parse_address()

    ip_address =
      case ip_address_result do
        {:ok, ip_address} ->
          ip_address

        _ ->
          Logger.warning("Received invalid IP address #{ip_address_string}.")
          {0, 0, 0, 0}
      end

    Queries.set_device_connected!(
      new_state.realm,
      new_state.device_id,
      DateTime.from_unix!(timestamp_ms, :millisecond),
      ip_address
    )

    TriggersHandler.device_connected(
      new_state.realm,
      new_state.device_id,
      new_state.groups,
      ip_address_string,
      timestamp_ms
    )

    Logger.info("Device connected.", ip_address: ip_address_string, tag: "device_connected")

    :telemetry.execute([:astarte, :data_updater_plant, :data_updater, :device_connection], %{}, %{
      realm: new_state.realm
    })

    new_state = %{new_state | connected: true, last_seen_message: timestamp}

    {:ack, :ok, new_state}
  end

  defp handle_install_volatile_trigger(%State{discard_messages: true} = state, _) do
    # Don't care
    {:ok, state}
  end

  defp handle_install_volatile_trigger(state, parent_id, trigger_id, simple_trigger, trigger_target) do
    Core.Trigger.handle_install_volatile_trigger(
      state,
      parent_id,
      trigger_id,
      simple_trigger,
      trigger_target
    )
  end

  def handle_delete_volatile_trigger(%State{discard_messages: true} = state, _) do
    # Don't care
    {:ok, state}
  end

  def handle_delete_volatile_trigger(state, trigger_id) do
    state = Core.Trigger.handle_delete_volatile_trigger(state, trigger_id)
    {:ok, state}
  end

  def handle_disconnection(state, timestamp) do
    new_state =
      state
      |> TimeBasedActions.execute_time_based_actions(timestamp)
      |> Core.Device.set_device_disconnected(timestamp)
      |> Map.put(:last_seen_message, timestamp)

    Logger.info("Device disconnected.", tag: "device_disconnected")

    {:ack, :ok, new_state}
  end

  def handle_internal(state, path, payload, timestamp) do
    Core.InternalHandler.handle_internal(state, path, payload, timestamp)
  end

  @impl true
  def handle_continue({:mississippi_error, context, error, opts}, _state) do
    new_state = Core.Error.continue_error(context, error, opts)
    {:ok, new_state}
  end

  @impl true
  def handle_continue({:processed_message, context}, state) do
    %{
      interface: interface,
      interface_descriptor: interface_descriptor,
      path: path,
      payload: payload
    } = context

    :telemetry.execute(
      [:astarte, :data_updater_plant, :data_updater, :processed_message],
      %{},
      %{
        realm: state.realm,
        interface_type: interface_descriptor.type
      }
    )

    new_state =
      Core.DataHandler.update_stats(
        state,
        interface,
        interface_descriptor.major_version,
        path,
        payload
      )

    {:ok, new_state}
  end

  def start_device_deletion(state, timestamp) do
    # Device deletion is among time-based actions
    new_state = TimeBasedActions.execute_time_based_actions(state, timestamp)

    {:ok, new_state}
  end

  def handle_data(%State{discard_messages: true} = state, _, _, _, _) do
    {:ack, :discard_messages, state}
  end

  def handle_data(state, interface, path, payload, timestamp) do
    TimeBasedActions.execute_time_based_actions(state, timestamp)
    |> Core.DataHandler.handle_data(interface, path, payload, timestamp)
  end

  defdelegate handle_capabilities(state, capabilities, timestamp),
    to: Core.CapabilitiesHandler

  defdelegate handle_control(state, path, payload, timestamp), to: Core.ControlHandler

  defdelegate handle_introspection(state, payload, timestamp),
    to: Core.IntrospectionHandler
end
