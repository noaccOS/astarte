modules = [
  :hackney,
  Astarte.DataAccess.Config,
  Astarte.DataAccess.FDO.Queries,
  Astarte.Secrets.Vault.Client,
  Astarte.Secrets.Config,
  Astarte.Secrets.Vault.Core
]

for module <- modules, do: Mimic.copy(module)

Astarte.Secrets.Config.init()

# fix flakiness due to async tests
Astarte.Secrets.Vault.Core.create_nested_namespace(["fdo_owner_keys", "default_instance"])
Astarte.Secrets.Vault.Core.create_nested_namespace(["fdo_owner_keys", "instance"])

ExUnit.start(capture_log: true)
