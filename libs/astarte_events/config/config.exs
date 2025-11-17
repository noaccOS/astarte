import Config

config :astarte_events, :cache_names, {:via, Horde.Registry, Registry.Triggers}
