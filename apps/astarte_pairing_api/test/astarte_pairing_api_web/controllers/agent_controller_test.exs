#
# This file is part of Astarte.
#
# Copyright 2017-2018 Ispirata Srl
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

defmodule Astarte.Pairing.APIWeb.AgentControllerTest do
  use Astarte.Pairing.APIWeb.ConnCase

  alias Astarte.Pairing.APIWeb.JWTTestHelper

  alias Astarte.RPC.Protocol.Pairing.{
    GenericErrorReply,
    GetAgentPublicKeyPEMsReply,
    RegisterDeviceReply,
    Reply
  }

  import Mox

  @agent_public_key_pems JWTTestHelper.agent_public_key_pems()

  @realm "testrealm"
  @hw_id "2imLILqtRP2vq0ZVy-TGRQ"
  @hw_id_256 "ova1YgZZZo3p_2m8UjJ_c3sOOmpLh3GOc0CARFwE-V4"
  @empty_invalid_hw_id ""
  @short_invalid_hw_id "YQ"
  @invalid_hw_id "5GGciygQUcHZqXyc1BNeC%"
  @existing_hw_id "o9RQb8B-R8WY_N7kR56M2w"

  @create_attrs %{"hw_id" => @hw_id}
  @create_attrs_256 %{"hw_id" => @hw_id_256}
  @invalid_attrs %{"hw_id" => @empty_invalid_hw_id}
  @short_invalid_attrs %{"hw_id" => @short_invalid_hw_id}
  @bad_encoding_invalid_attrs %{"hw_id" => @invalid_hw_id}
  @existing_attrs %{"hw_id" => @existing_hw_id}

  @credentials_secret "supersecret"

  @rpc_destination Astarte.RPC.Protocol.Pairing.amqp_queue()
  @timeout 30_000

  describe "register device" do
    @encoded_pubkey_response %Reply{
                               reply:
                                 {:get_agent_public_key_pems_reply,
                                  %GetAgentPublicKeyPEMsReply{
                                    agent_public_key_pems: @agent_public_key_pems
                                  }}
                             }
                             |> Reply.encode()

    @encoded_register_response %Reply{
                                 reply:
                                   {:register_device_reply,
                                    %RegisterDeviceReply{credentials_secret: @credentials_secret}}
                               }
                               |> Reply.encode()

    @encoded_already_registered_response %Reply{
                                           reply:
                                             {:generic_error_reply,
                                              %GenericErrorReply{error_name: "already_registered"}}
                                         }
                                         |> Reply.encode()

    setup %{conn: conn} do
      jwt = JWTTestHelper.gen_jwt_all_access_token()

      conn =
        conn
        |> put_req_header("accept", "application/json")
        |> put_req_header("authorization", "bearer #{jwt}")

      {:ok, conn: conn}
    end

    test "renders credentials_secret when data is valid", %{conn: conn} do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_register_response}
      end)

      conn = post(conn, agent_path(conn, :create, @realm), data: @create_attrs)
      assert %{"credentials_secret" => @credentials_secret} = json_response(conn, 201)["data"]
    end

    test "renders credentials_secret when data is valid and hardware id is 256 bits long", %{
      conn: conn
    } do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_register_response}
      end)

      conn = post(conn, agent_path(conn, :create, @realm), data: @create_attrs_256)
      assert %{"credentials_secret" => @credentials_secret} = json_response(conn, 201)["data"]
    end

    test "renders errors when data is invalid", %{conn: conn} do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)

      conn = post(conn, agent_path(conn, :create, @realm), data: @invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "renders errors when hardware id is too short", %{conn: conn} do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)

      conn = post(conn, agent_path(conn, :create, @realm), data: @short_invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "renders errors when hardware id is not valid base64", %{conn: conn} do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)

      conn = post(conn, agent_path(conn, :create, @realm), data: @bad_encoding_invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "renders errors when device already exists", %{conn: conn} do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_already_registered_response}
      end)

      conn = post(conn, agent_path(conn, :create, @realm), data: @existing_attrs)
      assert json_response(conn, 422)["errors"] == %{"error_name" => ["already_registered"]}
    end

    test "renders errors when unauthorized", %{conn: conn} do
      MockRPCClient
      |> expect(:rpc_call, fn _serialized_call, @rpc_destination, @timeout ->
        {:ok, @encoded_pubkey_response}
      end)

      conn =
        conn
        |> delete_req_header("authorization")
        |> post(agent_path(conn, :create, @realm), data: @create_attrs)

      assert json_response(conn, 401)["errors"] == %{"detail" => "Unauthorized"}
    end
  end
end
