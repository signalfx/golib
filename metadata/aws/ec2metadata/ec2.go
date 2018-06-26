package ec2metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const identityURL = "http://169.254.169.254/latest/dynamic/instance-identity/document"

// want mapping of desired aws metadata in aws identity document
var want = map[string]string{
	"availability_zone": "availabilityZone",
	"instance_type":     "instanceType",
	"instance_id":       "instanceId",
	"image_id":          "imageId",
	"account_id":        "accountId",
	"region":            "region",
	"architecture":      "architecture",
}

// New - returns a new EC2Metadata context
func New() *EC2Metadata {
	return &EC2Metadata{false, "", identityURL}
}

// EC2Metadata - checks and retrieves information about an aws EC2 instance
type EC2Metadata struct {
	isAWS       bool
	awsUniqueID string
	IdentityURL string
}

// Get - returns a map with aws metadata including the AWSUniqueID
func (s *EC2Metadata) Get() (map[string]string, error) {
	if identity, err := requestAWSInfo(s.IdentityURL); err == nil {
		s.isAWS = true
		info := s.processAWSInfo(identity)
		return info, nil
	}
	return nil, errors.New("not an aws box")
}

// buildAWSUniqueID takes a map of aws metadata and manufactures an awsUniqueId
func buildAWSUniqueID(info map[string]string) (awsUniqueID string) {
	if id, ok := info["aws_instance_id"]; ok {
		if region, ok := info["aws_region"]; ok {
			if account, ok := info["aws_account_id"]; ok {
				awsUniqueID = fmt.Sprintf("%s_%s_%s", id, region, account)
			}
		}
	}
	return awsUniqueID
}

// processAWSInfo maps the aws identity document to metadata keys and values
func (s *EC2Metadata) processAWSInfo(identity map[string]interface{}) (info map[string]string) {
	info = make(map[string]string, len(want)+1) // +1 to make room for AWSUniqueId

	// extract desired metadata
	for k, v := range want {
		// if a value exists add it to the host info
		if val, ok := identity[v]; ok {
			info[fmt.Sprintf("aws_%s", k)] = val.(string)
		}
	}

	// build aws unique id if it hasn't been set
	if s.awsUniqueID == "" {
		s.awsUniqueID = buildAWSUniqueID(info)
	}

	// set uniqueId
	info["AWSUniqueId"] = s.awsUniqueID

	return info
}

// requestAWSInfo makes a request to the desired aws metadata url and marshalls
// the response to a map[string]interface{}
func requestAWSInfo(url string) (identity map[string]interface{}, err error) {
	identity = map[string]interface{}{}
	httpClient := &http.Client{Timeout: 200 * time.Millisecond}

	// make the request
	var res *http.Response
	if res, err = httpClient.Get(url); err == nil {
		// read the response
		var raw []byte
		if raw, err = ioutil.ReadAll(res.Body); err == nil {
			// parse the json response
			err = json.Unmarshal(raw, &identity)
		}
	}

	return identity, err
}
