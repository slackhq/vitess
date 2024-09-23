name: {{.Name}}
on: [push, pull_request]

permissions: read-all

env:
  GOPRIVATE: github.com/slackhq/vitess-addons
  GH_ACCESS_TOKEN: "${{`{{ secrets.GH_ACCESS_TOKEN }}`}}"

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on:
      group: vitess-ubuntu20

    steps:
    - name: Skip CI
      run: |
        if [[ "{{"${{contains( github.event.pull_request.labels.*.name, 'Skip CI')}}"}}" == "true" ]]; then
          echo "skipping CI due to the 'Skip CI' label"
          exit 1
        fi

    - name: Check if workflow needs to be skipped
      id: skip-workflow
      run: |
        skip='false'
        echo Skip ${skip}
        echo "skip-workflow=${skip}" >> $GITHUB_OUTPUT

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v4

    - name: Check for changes in relevant files
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: dorny/paths-filter@v3.0.1
      id: changes
      with:
        token: ''
        filters: |
          end_to_end:
            - 'go/**/*.go'
            - 'go/vt/sidecardb/**/*.sql'
            - 'go/test/endtoend/onlineddl/vrepl_suite/**'
            - 'test.go'
            - 'Makefile'
            - 'build.env'
            - 'go.sum'
            - 'go.mod'
            - 'proto/*.proto'
            - 'tools/**'
            - 'config/**'
            - 'bootstrap.sh'
            - '.github/workflows/{{.FileName}}'

    - name: Set up Go
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-go@v5
      with:
        go-version: 1.22.7

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      run: |
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 65535"

    - name: Run cluster endtoend test
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 30
      run: |
        # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
        # which musn't be more than 107 characters long.
        export VTDATAROOT="/tmp/"

        go run test.go -docker=true --follow -shard {{.Shard}}
