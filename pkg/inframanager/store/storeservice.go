// Copyright (c) 2022 Intel Corporation.  All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"fmt"
	"net"
	"os"
	"reflect"

	log "github.com/sirupsen/logrus"
)

const (
	servicesFile = storePath + "services_db.json"
)

func IsServiceStoreEmpty() bool {
	if len(ServiceSet.ServiceMap) == 0 {
		return true
	} else {
		return false
	}
}

func InitServiceStore(setFwdPipe bool) bool {
	flags := os.O_CREATE

	/*
	   Initialize the store to empty while setting the
	   forwarding pipeline. It indicates that the p4-ovs
	   server has just started and pipeline is set.
	   And no stale forwarding rules should exist in the store.
	   Truncate if any entries from previous server runs.
	*/
	if setFwdPipe {
		flags = flags | os.O_TRUNC
	}

	/* Create the store file if it doesn't exist */
	file, err := NewOpenFile(ServicesFile, flags, 0600)
	if err != nil {
		log.Error("Failed to open", ServicesFile)
		return false
	}
	file.Close()

	data, err := NewReadFile(ServicesFile)
	if err != nil {
		log.Error("Error reading ", ServicesFile, err)
		return false
	}

	if len(data) == 0 {
		return true
	}

	err = JsonUnmarshal(data, &ServiceSet.ServiceMap)
	if err != nil {
		log.Error("Error unmarshalling data from ", ServicesFile, err)
		return false
	}

	return true
}

func (s Service) WriteToStore() bool {
	if net.ParseIP(s.ClusterIp) == nil {
		log.Errorf("Invalid cluster IP %s", s.ClusterIp)
		return false
	}
	//aquire lock before adding entry into the map
	ServiceSet.ServiceLock.Lock()
	//append tmp entry to the map
	ServiceSet.ServiceMap[s.ClusterIp] = s
	//release lock after updating the map
	ServiceSet.ServiceLock.Unlock()
	return true
}

func (s Service) DeleteFromStore() bool {
	if net.ParseIP(s.ClusterIp) == nil {
		log.Errorf("Invalid cluster IP %s", s.ClusterIp)
		return false
	}

	res := ServiceSet.ServiceMap[s.ClusterIp]
	if reflect.DeepEqual(res, Service{}) {
		log.Errorf("corresponding service entry is not found in the store for %s", s.ClusterIp)
		return false
	}

	//aquire lock before adding entry into the map
	ServiceSet.ServiceLock.Lock()
	//delete tmp entry from the map
	delete(ServiceSet.ServiceMap, s.ClusterIp)
	//release lock after updating the map
	ServiceSet.ServiceLock.Unlock()
	return true
}

func (s Service) GetFromStore() store {
	if net.ParseIP(s.ClusterIp) == nil {
		log.Errorf("Invalid cluster IP %s", s.ClusterIp)
		return nil
	}

	res := ServiceSet.ServiceMap[s.ClusterIp]
	if reflect.DeepEqual(res, Service{}) {
		return nil
	} else {
		return res
	}
}

func (s Service) UpdateToStore() bool {
	fmt.Println("not implemented")
	return true
}

func RunSyncServiceInfo() bool {
	jsonStr, err := JsonMarshalIndent(ServiceSet.ServiceMap, "", " ")
	if err != nil {
		log.Errorf("Failed to marshal service entries map %s", err)
		return false
	}

	if err = NewWriteFile(ServicesFile, jsonStr, 0600); err != nil {
		log.Errorf("Failed to write entries to %s, err %s",
			ServicesFile, err)
		return false
	}
	return true
}
