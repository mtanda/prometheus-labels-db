{ pkgs ? import <nixpkgs> {}}:

pkgs.mkShell {
  hardeningDisable = [ "fortify" ];
  packages = [
    pkgs.go
    pkgs.gopls
    pkgs.gotests
    pkgs.delve
    pkgs.pcre
    pkgs.sqlite
    pkgs.goreleaser
  ];
}
