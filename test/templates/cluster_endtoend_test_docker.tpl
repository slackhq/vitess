name: {{.Name}}
on: [push, pull_request]

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on:
      group: vitess-ubuntu20
    env:
      GOPRIVATE: github.com/slackhq/vitess-addons
      GH_ACCESS_TOKEN: {{"${{ secrets.GH_ACCESS_TOKEN }}"}}

    steps:
    - name: Check if workflow needs to be skipped
      id: skip-workflow
      run: |
        skip='false'
        if [[ "{{"${{github.event.pull_request}}"}}" ==  "" ]] && [[ "{{"${{github.ref}}"}}" != "refs/heads/main" ]] && [[ ! "{{"${{github.ref}}"}}" =~ ^refs/heads/slack-vitess-r[0-9]+\.[0-9]$ ]] && [[ ! "{{"${{github.ref}}"}}" =~ "refs/tags/.*" ]]; then
          skip='true'
        fi
        echo Skip ${skip}
        echo "skip-workflow=${skip}" >> $GITHUB_OUTPUT

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v3

    - name: Check for changes in relevant files
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: frouioui/paths-filter@main
      id: changes
      with:
        token: ''
        filters: |
          end_to_end:
            - 'go/**/*.go'
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
      uses: actions/setup-go@v3
      with:
        go-version: 1.21.8

    - name: Setup github.com/slackhq/vitess-additions access token
      run: git config --global url.https://$GH_ACCESS_TOKEN@github.com/.insteadOf https://github.com/

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    - name: Run cluster endtoend test
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 30
      run: |
        go run test.go -docker=true --follow -shard {{.Shard}}
