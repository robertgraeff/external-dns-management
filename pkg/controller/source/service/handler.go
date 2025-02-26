/*
 * Copyright 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *
 */

package service

import (
	"fmt"

	"github.com/gardener/controller-manager-library/pkg/logger"
	"github.com/gardener/controller-manager-library/pkg/resources"
	"github.com/gardener/controller-manager-library/pkg/utils"
	"github.com/gardener/external-dns-management/pkg/dns"
	api "k8s.io/api/core/v1"
)

func GetTargets(logger logger.LogContext, obj resources.Object, names dns.DNSNameSet) (utils.StringSet, utils.StringSet, error) {
	svc := obj.Data().(*api.Service)
	if svc.Spec.Type != api.ServiceTypeLoadBalancer {
		if len(names) == 0 {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("service is not of type LoadBalancer")
	}
	set := utils.StringSet{}
	for _, i := range svc.Status.LoadBalancer.Ingress {
		if i.Hostname != "" && i.IP == "" {
			set.Add(i.Hostname)
		} else {
			if i.IP != "" {
				set.Add(i.IP)
			}
		}
	}
	return set, nil, nil
}
