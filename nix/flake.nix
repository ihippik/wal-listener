{
  description = "wal-listener development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    (flake-utils.lib.eachDefaultSystem (
      system:
      nixpkgs.lib.fix (
        flake:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          packages = {
            direnv = pkgs.direnv;
            git = pkgs.git;
            google-cloud-sdk = pkgs.google-cloud-sdk.withExtraComponents ([
              pkgs.google-cloud-sdk.components.gke-gcloud-auth-plugin
              pkgs.google-cloud-sdk.components.pubsub-emulator
            ]);
            postgresql = pkgs.postgresql_13;
          };

          devShell = pkgs.mkShell {
            packages = builtins.attrValues flake.packages;
          };
        }
      )
    ));
}
