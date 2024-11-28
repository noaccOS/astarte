{
  description = "Open Source IoT platform focused on Data management and processing";
  inputs = {
    nixpkgs.url = "nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
    elixir-utils = {
      url = "github:noaccOS/elixir-utils";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };
  outputs =
    {
      nixpkgs,
      elixir-utils,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachSystem elixir-utils.lib.defaultSystems (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.callPackage elixir-utils.lib.asdfDevShell {
          toolVersions = ./.tool-versions;
          packages = [ pkgs.gleam ];
          wxSupport = false;
        };
        formatter = pkgs.nixpkgs-fmt;
      }
    );
}
