package ec2metadata

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func Test_buildAWSUniqueID(t *testing.T) {
	type args struct {
		info map[string]string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Validate that AWSUniqueID can build",
			args: args{
				info: map[string]string{
					"aws_instance_id": "hello",
					"aws_region":      "world",
					"aws_account_id":  "too",
				},
			},
			want: "hello_world_too",
		},
		{
			name: "AWSUniqueID missing aws_instance_id should produce error",
			args: args{
				info: map[string]string{
					"aws_region":     "world",
					"aws_account_id": "too",
				},
			},
			want: "",
		},
		{
			name: "AWSUniqueID missing aws_region should produce error",
			args: args{
				info: map[string]string{
					"aws_instance_id": "hello",
					"aws_account_id":  "too",
				},
			},
			want: "",
		},
		{
			name: "AWSUniqueID missing aws_account_id should produce error",
			args: args{
				info: map[string]string{
					"aws_region":      "world",
					"aws_instance_id": "hello",
				},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildAWSUniqueID(tt.args.info)
			if got != tt.want {
				t.Errorf("buildAWSUniqueID() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// test fixture to mock aws identity document server
type requestHandler struct {
	response string
}

func (rh *requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, rh.response)
}

func TestAWSMetadata_Get(t *testing.T) {
	tests := []struct {
		name     string
		identity map[string]interface{}
		want     map[string]string
		wantErr  bool
	}{
		{
			name: "Test successful connection",
			identity: map[string]interface{}{
				"availabilityZone": "testAZ",
				"instanceType":     "testInstanceType",
				"instanceId":       "testInstanceId",
				"imageId":          "testImageId",
				"accountId":        "testAccountId",
				"region":           "testRegion",
				"architecture":     "testArchitecture",
			},
			want: map[string]string{
				"aws_availability_zone": "testAZ",
				"aws_instance_type":     "testInstanceType",
				"aws_instance_id":       "testInstanceId",
				"aws_image_id":          "testImageId",
				"aws_account_id":        "testAccountId",
				"aws_region":            "testRegion",
				"aws_architecture":      "testArchitecture",
				"AWSUniqueId":           "testInstanceId_testRegion_testAccountId",
			},
			wantErr: false,
		},
		{
			name:     "Test unsuccessful connection",
			identity: map[string]interface{}{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create new aws metadata
			awsMeta := New()

			if !tt.wantErr {
				// marshall identity into json
				js, _ := json.Marshal(tt.identity)

				// set up mock server
				handler := &requestHandler{response: string(js)}
				server := httptest.NewServer(handler)
				defer server.Close()

				// set the identityURL to point to mock server
				awsMeta.IdentityURL = server.URL
			} else {
				awsMeta.IdentityURL = "NotARealDomainName"
			}

			got, err := awsMeta.Get()
			if (err != nil) != tt.wantErr {
				t.Errorf("EC2Metadata.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EC2Metadata.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
