/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpcclient

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestAppendStaticAuth(t *testing.T) {
	{
		clientCreds = nil
		clientCredsErr = nil
		opts, err := AppendStaticAuth([]grpc.DialOption{})
		assert.Nil(t, err)
		assert.Len(t, opts, 0)
	}
	{
		clientCreds = nil
		clientCredsErr = errors.New("test err")
		opts, err := AppendStaticAuth([]grpc.DialOption{})
		assert.NotNil(t, err)
		assert.Len(t, opts, 0)
	}
	{
		clientCreds = &StaticAuthClientCreds{Username: "test", Password: "123456"}
		clientCredsErr = nil
		opts, err := AppendStaticAuth([]grpc.DialOption{})
		assert.Nil(t, err)
		assert.Len(t, opts, 1)
	}
}

func TestLoadStaticAuthCredsFromFile(t *testing.T) {
	{
		f, err := os.CreateTemp("", t.Name())
		if !assert.Nil(t, err) {
			assert.FailNowf(t, "cannot create temp file: %s", err.Error())
		}
		_, err = f.Write([]byte(`{
			"Username": "test",
			"Password": "correct horse battery staple"
		}`))
		if !assert.Nil(t, err) {
			assert.FailNowf(t, "cannot read auth file: %s", err.Error())
		}
		defer os.Remove(f.Name())

		creds, err := loadStaticAuthCredsFromFile(f.Name())
		assert.Nil(t, err)
		assert.Equal(t, "test", creds.Username)
		assert.Equal(t, "correct horse battery staple", creds.Password)
	}
	{
		_, err := loadStaticAuthCredsFromFile(`does-not-exist`)
		assert.NotNil(t, err)
	}
}

func TestHandleStaticAuthCredsFileSignals(t *testing.T) {
	tmp, err := os.CreateTemp("", t.Name())
	assert.Nil(t, err)
	defer os.Remove(tmp.Name())
	credsFileName := tmp.Name()
	credsFile = &credsFileName

	// load old creds
	fmt.Fprintln(tmp, `{"Username": "old", "Password": "123456"}`)
	ResetStaticAuth()
	_, _ = AppendStaticAuth([]grpc.DialOption{})

	// write new creds to the same file
	_ = tmp.Truncate(0)
	_, _ = tmp.Seek(0, 0)
	fmt.Fprintln(tmp, `{"Username": "new", "Password": "123456789"}`)

	// test the creds did not change yet
	_, _ = AppendStaticAuth([]grpc.DialOption{})
	assert.Equal(t, &StaticAuthClientCreds{Username: "old", Password: "123456"}, clientCreds)

	// test SIGHUP signal triggers reload
	reloadedChan := make(chan bool, 1)
	go func(reloadedChan chan bool) {
		clientCredsOld := clientCreds
		for {
			select {
			case <-time.After(time.Second * 15):
				reloadedChan <- false
				return
			default:
				clientCredsMu.Lock()
				if !reflect.DeepEqual(clientCreds, clientCredsOld) {
					reloadedChan <- true
					return
				}
				clientCredsMu.Unlock()
			}
		}
	}(reloadedChan)
	clientCredsSigChan <- syscall.SIGHUP
	assert.True(t, <-reloadedChan)
	assert.Nil(t, clientCredsErr)
	assert.Equal(t, &StaticAuthClientCreds{Username: "new", Password: "123456789"}, clientCreds)
}
