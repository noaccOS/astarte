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

defmodule Astarte.Secrets.FDOOwnerKeys.InitOptions do
  @moduledoc """
  Schema and validation for owner key upload/creation options in Astarte API.
  Defines the parameters that can be used when requesting creation or upload of an owner key in OpenBao.
  """
  use TypedEctoSchema
  import Ecto.Changeset

  alias Astarte.Secrets.FDOOwnerKeys.InitOptions
  alias Astarte.Secrets.Vault.Core
  alias COSE.Keys

  typed_embedded_schema do
    field :action, Ecto.Enum, values: [:create, :upload]
    field :key_name, :string
    field :key_data, :string
    field(:key, :any, virtual: true) :: COSE.Keys.Key.t()
    field :key_algorithm, Ecto.Enum, values: Core.key_algorithm_enum()
  end

  @doc false
  def changeset(%InitOptions{} = owner_key_request, attrs) do
    cast_attrs = [
      :action,
      :key_name,
      :key_data,
      :key_algorithm
    ]

    required_attrs = [
      :action,
      :key_name
    ]

    owner_key_request
    |> cast(attrs, cast_attrs)
    |> validate_required(required_attrs)
    |> validate_conditional_attrs()
  end

  # key creation and upload require different parameters
  defp validate_conditional_attrs(changeset) do
    case get_change(changeset, :action) do
      :create ->
        changeset
        |> validate_required(:key_algorithm)

      :upload ->
        changeset
        |> validate_required(:key_data)
        |> parse_key(:key_data, :key)

      _ ->
        changeset
    end
  end

  defp parse_key(%{valid?: false} = changeset, _json_field, _struct_field), do: changeset

  defp parse_key(changeset, json_field, struct_field) do
    # SAFETY: changeset is valid, so there must be the json field
    json_key = fetch_field!(changeset, json_field)

    case Keys.from_pem(json_key) do
      {:ok, key} -> put_change(changeset, struct_field, key)
      :error -> add_error(changeset, json_field, "is not a valid ECC or RSA key in PEM format")
    end
  end
end
