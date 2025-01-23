{
  description = "Open Source IoT platform focused on Data management and processing";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";

    elixir-utils = {
      url = "github:noaccOS/elixir-utils";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-parts.follows = "flake-parts";
    };
  };
  outputs =
    inputs@{
      self,
      flake-parts,
      elixir-utils,
      nixpkgs,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = elixir-utils.lib.defaultSystems;
      flake.overlays.default = elixir-utils.lib.asdfOverlay {
        toolVersions = ./.tool-versions;
        wxSupport = false;
      };
      perSystem =
        { pkgs, system, ... }:
        {
          _module.args.pkgs = import nixpkgs {
            inherit system;
            overlays = [ self.overlays.default ];
          };

          devShells.default = pkgs.callPackage elixir-utils.lib.devShell {
            packages = [ pkgs.gleam ];
            shellHook = ''
              mix archive.install hex mix_gleam --force
            '';
          };
          formatter = pkgs.nixfmt-rfc-style;
        };
    };
}
