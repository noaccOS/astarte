#
# This file is part of Astarte.
#
# Copyright 2026 SECO Mind Srl
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

defmodule Astarte.FDO.Config.BaseURLHost do
  @moduledoc """
  Custom Skogsra type for the FDO base URL host.

  The configured value is parsed once here and resolved to a tagged tuple:

    * `{:ip, :inet.ip_address()}` when the value parses as an IP address
    * `{:domain, String.t()}` otherwise
  """

  use Skogsra.Type

  import Kernel, except: [to_string: 1]

  @typedoc "The resolved base URL host."
  @type host :: {:domain, String.t()} | {:ip, :inet.ip_address()}

  @impl Skogsra.Type
  def cast(value) when is_binary(value) and value != "" do
    case value |> String.to_charlist() |> :inet.parse_address() do
      {:ok, address} -> {:ok, {:ip, address}}
      {:error, _reason} -> {:ok, {:domain, value}}
    end
  end

  def cast({:domain, domain} = value) when is_binary(domain) and domain != "", do: {:ok, value}

  def cast({:ip, address} = value) when is_tuple(address), do: {:ok, value}

  def cast(_), do: :error

  @doc """
  Renders a resolved host back to its string form, e.g. for URL composition.
  """
  @spec to_string(host()) :: String.t()
  def to_string({:domain, domain}), do: domain
  def to_string({:ip, address}), do: address |> :inet.ntoa() |> List.to_string()
end
