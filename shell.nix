{ pkgs ? import <nixpkgs> {}}:

pkgs.mkShell {
  hardeningDisable = [ "fortify" ];
  packages = [
    pkgs.go
    pkgs.gopls
    pkgs.delve
  ];
}
