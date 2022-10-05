# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.

{ busybox
, dockerTools
, lib
, utillinux
, control-plane
, tini
, img_tag ? ""
}:
let
  tag = if img_tag != "" then img_tag else control-plane.version;
  build-control-plane-image = { build, name, config ? { } }: dockerTools.buildImage {
    inherit tag;
    created = "now";
    name = "mayadata/mcp-${name}";
    contents = [ tini busybox control-plane.${build}.${name} ];
    config = { Entrypoint = [ "tini" "--" control-plane.${build}.${name}.binary ]; } // config;
  };
  build-agent-image = { build, name, config ? { } }: build-control-plane-image {
    inherit build name;
  };
  build-rest-image = { build }: build-control-plane-image {
    inherit build;
    name = "rest";
    config = { ExposedPorts = { "8080/tcp" = { }; "8081/tcp" = { }; }; };
  };
  build-msp-operator-image = { build }: build-control-plane-image {
    inherit build;
    name = "msp-operator";
  };
  build-csi-controller-image = { build }: build-control-plane-image {
    inherit build;
    name = "csi-controller";
  };
in
{
  core = build-agent-image { build = "release"; name = "core"; };
  core-dev = build-agent-image { build = "debug"; name = "core"; };
  jsongrpc = build-agent-image { build = "release"; name = "jsongrpc"; };
  jsongrpc-dev = build-agent-image { build = "debug"; name = "jsongrpc"; };
  rest = build-rest-image {
    build = "release";
  };
  rest-dev = build-rest-image {
    build = "debug";
  };

  msp-operator = build-msp-operator-image {
    build = "release";
  };
  msp-operator-dev = build-msp-operator-image {
    build = "debug";
  };

  csi-controller = build-csi-controller-image {
    build = "release";
  };
  csi-controller-dev = build-csi-controller-image {
    build = "debug";
  };
}
