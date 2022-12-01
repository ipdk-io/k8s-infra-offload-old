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

package policy

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/ipdk-io/k8s-infra-offload/pkg/types"
	proto "github.com/ipdk-io/k8s-infra-offload/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/tomb.v2"
)

var (
	grpcDial            = grpc.Dial
	newInfraAgentClient = proto.NewInfraPolicyClient
	cancellableListener = getCancellableListener
	removeSocket        = os.RemoveAll
)

type PolicyServer struct {
	log           *logrus.Entry
	nextSeqNumber uint64
	exiting       chan bool
	name          string
}

func NewPolicyServer(log *logrus.Entry) (types.Server, error) {
	return &PolicyServer{
		log:           log,
		nextSeqNumber: 0,
		exiting:       make(chan bool),
		name:          "felix-policy-server"}, nil
}

func (s *PolicyServer) GetName() string {
	return s.name
}

func (s *PolicyServer) SyncPolicy(conn net.Conn) {
	for {
		msg, err := s.RecvMessage(conn)
		if err != nil {
			s.log.WithError(err).Warn("error communicating with felix")
			conn.Close()
			return
		}
		s.log.Infof("Got message from felix %T", msg)
		switch m := msg.(type) {
		case *proto.ConfigUpdate:
			err = s.handleConfigUpdate(m)
		case *proto.InSync:
			err = s.handleInSyc(m)
		default:
			err = s.handleMessage(msg, false)
		}

		if err != nil {
			s.log.WithError(err).Warn("Error processing update from felix, restarting")
			conn.Close()
			return
		}

	}
}

func (s *PolicyServer) handleMessage(msg interface{}, pending bool) error {
	switch m := msg.(type) {
	case *proto.IPSetUpdate:
		return s.handleIpsetUpdate(m, pending)
	case *proto.IPSetDeltaUpdate:
		return s.handleIpsetDeltaUpdate(m, pending)
	case *proto.IPSetRemove:
		return s.handleIpsetRemove(m, pending)
	case *proto.ActivePolicyUpdate:
		return s.handleActivePolicyUpdate(m, pending)
	case *proto.ActivePolicyRemove:
		return s.handleActivePolicyRemove(m, pending)
	case *proto.ActiveProfileUpdate:
		return s.handleActiveProfileUpdate(m, pending)
	case *proto.ActiveProfileRemove:
		return s.handleActiveProfileRemove(m, pending)
	case *proto.HostEndpointUpdate:
		return s.handleHostEndpointUpdate(m, pending)
	case *proto.HostEndpointRemove:
		return s.handleHostEndpointRemove(m, pending)
	case *proto.WorkloadEndpointUpdate:
		return s.handleWorkloadEndpointUpdate(m, pending)
	case *proto.WorkloadEndpointRemove:
		return s.handleWorkloadEndpointRemove(m, pending)
	case *proto.HostMetadataUpdate:
		return s.handleHostMetadataUpdate(m, pending)
	case *proto.HostMetadataRemove:
		return s.handleHostMetadataRemove(m, pending)
	case *proto.IPAMPoolUpdate:
		return s.handleIpamPoolUpdate(m, pending)
	case *proto.IPAMPoolRemove:
		return s.handleIpamPoolRemove(m, pending)
	case *proto.ServiceAccountUpdate:
		return s.handleServiceAccountUpdate(m, pending)
	case *proto.ServiceAccountRemove:
		return s.handleServiceAccountRemove(m, pending)
	case *proto.NamespaceUpdate:
		return s.handleNamespaceUpdate(m, pending)
	case *proto.NamespaceRemove:
		return s.handleNamespaceRemove(m, pending)
	case *proto.RouteUpdate:
		return s.handleRouteUpdate(m, pending)
	case *proto.RouteRemove:
		return s.handleRouteRemove(m, pending)
	case *proto.VXLANTunnelEndpointRemove:
		return s.handleVXLANTunnelEndpointRemove(m, pending)
	case *proto.VXLANTunnelEndpointUpdate:
		return s.handleVXLANTunnelEndpointUpdate(m, pending)
	case *proto.WireguardEndpointUpdate:
		return s.handleWireguardEndpointUpdate(m, pending)
	case *proto.WireguardEndpointRemove:
		return s.handleWireguardEndpointRemove(m, pending)
	case *proto.GlobalBGPConfigUpdate:
		return s.handleGlobalBGPConfigUpdate(m, pending)
	default:
		s.log.Warnf("Unhandled message from felix: %v", m)
	}
	return nil
}

func (s *PolicyServer) StopServer() {
	s.exiting <- true
}

// Not needed?
func (s *PolicyServer) handleConfigUpdate(msg *proto.ConfigUpdate) error {
	s.log.Infof("Got config update %+v", msg)
	return nil
}

// Not needed?
func (s *PolicyServer) handleInSyc(msg *proto.InSync) error {
	s.log.Infof("Got in sync %+v", msg)
	return nil
}

func (s *PolicyServer) handleIpsetUpdate(msg *proto.IPSetUpdate, pending bool) error {
	s.log.Infof("Got ipset update %+v pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleIpsetUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateIPSet(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleIpsetUpdate")
	}
	return nil
}

func (s *PolicyServer) handleIpsetDeltaUpdate(msg *proto.IPSetDeltaUpdate, pending bool) error {
	s.log.Infof("Got ipset delta update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleIpsetDeltaUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateIPSetDelta(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleIpsetDeltaUpdate")
	}
	return nil
}

func (s *PolicyServer) handleIpsetRemove(msg *proto.IPSetRemove, pending bool) error {
	s.log.Infof("Got ipset remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleIpsetRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveIPSet(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleIpsetRemove")
	}
	return nil
}

func (s *PolicyServer) handleActivePolicyUpdate(msg *proto.ActivePolicyUpdate, pending bool) error {
	s.log.Infof("Got active police update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleActivePolicyUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.ActivePolicyUpdate(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleActivePolicyUpdate")
	}
	return nil
}

func (s *PolicyServer) handleActivePolicyRemove(msg *proto.ActivePolicyRemove, pending bool) error {
	s.log.Infof("Got active police remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleActivePolicyRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.ActivePolicyRemove(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleActivePolicyRemove")
	}
	return nil
}

func (s *PolicyServer) handleActiveProfileUpdate(msg *proto.ActiveProfileUpdate, pending bool) error {
	s.log.Infof("Got active profile update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleActiveProfileUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateActiveProfile(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleActiveProfileUpdate")
	}
	return nil
}

func (s *PolicyServer) handleActiveProfileRemove(msg *proto.ActiveProfileRemove, pending bool) error {
	s.log.Infof("Got active profile remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleActiveProfileRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveActiveProfile(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleActiveProfileRemove")
	}
	return nil
}

func (s *PolicyServer) handleHostEndpointUpdate(msg *proto.HostEndpointUpdate, pending bool) error {
	s.log.Infof("Got host endpoint update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleHostEndpointUpdate: cannot dial manager")
	}

	out, err = c.UpdateHostEndpoint(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleHostEndpointUpdate")
	}
	return nil
}

func (s *PolicyServer) handleHostEndpointRemove(msg *proto.HostEndpointRemove, pending bool) error {
	s.log.Infof("Got host endpoint remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleHostEndpointRemove: cannot dial manager")
	}

	out, err = c.RemoveHostEndpoint(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleHostEndpointRemove")
	}
	return nil
}

func (s *PolicyServer) handleWorkloadEndpointUpdate(msg *proto.WorkloadEndpointUpdate, pending bool) error {
	s.log.Infof("Got workload endpoint update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleWorkloadEndpointUpdate: cannot dial manager")
	}

	out, err = c.UpdateLocalEndpoint(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleWorkloadEndpointUpdate")
	}
	return nil
}

func (s *PolicyServer) handleWorkloadEndpointRemove(msg *proto.WorkloadEndpointRemove, pending bool) error {
	s.log.Infof("Got workload endpoint remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleWorkloadEndpointRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveLocalEndpoint(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleWorkloadEndpointRemove")
	}
	return nil
}

func (s *PolicyServer) handleHostMetadataUpdate(msg *proto.HostMetadataUpdate, pending bool) error {
	s.log.Infof("Got host metadata update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleHostMetadataUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateHostMetaData(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleHostMetadataUpdate")
	}
	return nil
}

func (s *PolicyServer) handleHostMetadataRemove(msg *proto.HostMetadataRemove, pending bool) error {
	s.log.Infof("Got host metadata remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleHostMetadataRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveHostMetaData(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleHostMetadataRemove")
	}
	return nil
}

// Not needed?
func (s *PolicyServer) handleIpamPoolUpdate(msg *proto.IPAMPoolUpdate, pending bool) error {
	s.log.Infof("Got ipam pool update %+v, pending %v", msg, pending)
	return nil
}

// Not needed?
func (s *PolicyServer) handleIpamPoolRemove(msg *proto.IPAMPoolRemove, pending bool) error {
	s.log.Infof("Got ipam pool remove %+v, pending %v", msg, pending)
	return nil
}

func (s *PolicyServer) handleServiceAccountUpdate(msg *proto.ServiceAccountUpdate, pending bool) error {
	s.log.Infof("Got service account update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleServiceAccountUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateServiceAccount(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleServiceAccountUpdate")
	}
	return nil
}

func (s *PolicyServer) handleServiceAccountRemove(msg *proto.ServiceAccountRemove, pending bool) error {
	s.log.Infof("Got service account remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleServiceAccountRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveServiceAccount(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleServiceAccountRemove")
	}
	return nil
}

func (s *PolicyServer) handleNamespaceUpdate(msg *proto.NamespaceUpdate, pending bool) error {
	s.log.Infof("Got namespace update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleNamespaceUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateNamespace(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleNamespaceUpdate")
	}
	return nil
}

func (s *PolicyServer) handleNamespaceRemove(msg *proto.NamespaceRemove, pending bool) error {
	s.log.Infof("Got namespace remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleNamespaceRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveNamespace(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleNamespaceRemove")
	}
	return nil
}

func (s *PolicyServer) handleRouteUpdate(msg *proto.RouteUpdate, pending bool) error {
	s.log.Infof("Got route update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleRouteUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateRoute(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleRouteUpdate")
	}
	return nil
}

func (s *PolicyServer) handleRouteRemove(msg *proto.RouteRemove, pending bool) error {
	s.log.Infof("Got route remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleRouteRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveRoute(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleRouteRemove")
	}
	return nil
}

func (s *PolicyServer) handleVXLANTunnelEndpointUpdate(msg *proto.VXLANTunnelEndpointUpdate, pending bool) error {
	s.log.Infof("Got VXLAN tunnel endpoint update %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleVXLANTunnelEndpointUpdate: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.UpdateVXLANTunnelEndpoint(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleVXLANTunnelEndpointUpdate")
	}
	return nil
}

func (s *PolicyServer) handleVXLANTunnelEndpointRemove(msg *proto.VXLANTunnelEndpointRemove, pending bool) error {
	s.log.Infof("Got VXLAN tunnel endpoint remove %+v, pending %v", msg, pending)
	out := &proto.Reply{
		Successful: true,
	}
	c, err := s.dialManager()
	if err != nil {
		return errors.Wrap(err, "cannot process handleVXLANTunnelEndpointRemove: cannot dial manager")
	}
	// TODO: Add pending flag
	out, err = c.RemoveVXLANTunnelEndpoint(context.TODO(), msg)
	if err != nil || !out.Successful {
		return errors.Wrap(err, "cannot process handleVXLANTunnelEndpointRemove")
	}
	return nil
}

func (s *PolicyServer) handleWireguardEndpointUpdate(msg *proto.WireguardEndpointUpdate, pending bool) error {
	s.log.Infof("Got Wireguard endpoint update %+v, pending %v", msg, pending)
	return nil
}

func (s *PolicyServer) handleWireguardEndpointRemove(msg *proto.WireguardEndpointRemove, pending bool) error {
	s.log.Infof("Got Wireguard endpoint remove %+v, pending %v", msg, pending)
	return nil
}

func (s *PolicyServer) handleGlobalBGPConfigUpdate(msg *proto.GlobalBGPConfigUpdate, pending bool) error {
	s.log.Infof("Got GlobalBGPConfig update %+v, pending %v", msg, pending)
	return nil
}

func (s *PolicyServer) dialManager() (proto.InfraPolicyClient, error) {
	managerAddr := fmt.Sprintf("%s:%s", types.InfraManagerAddr, types.InfraManagerPort)
	conn, err := grpcDial(managerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.log.WithField("func", "dialManager")
		s.log.Errorf("unable to dial Infra Manager. err %v", err)
		return nil, err
	}
	return newInfraAgentClient(conn), nil
}

func getCancellableListener(ctx context.Context) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "unix", types.FelixDataplaneSocket)
}

func (s *PolicyServer) Start(t *tomb.Tomb) error {
	s.log.Info("Starting policy server")
	_ = removeSocket(types.FelixDataplaneSocket)
	waitCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.TODO())
	listener, err := cancellableListener(ctx)
	if err != nil {
		s.log.WithError(err).Errorf("Could not bind to %s", types.FelixDataplaneSocket)
		cancel()
		return err
	}
	go func() {
		defer close(waitCh)
		<-ctx.Done()
		if listener != nil {
			listener.Close()
		}
		_ = removeSocket(types.FelixDataplaneSocket)
	}()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					// error due context cancelation
					return
				default:
					s.log.WithError(err).Warn("cannot accept policy connection")
					return
				}
			}
			go s.SyncPolicy(conn)

			s.log.Info("Waiting to close...")
			<-s.exiting
			if err = conn.Close(); err != nil {
				s.log.WithError(err).Error("error closing conection to felix API proxy")
			}
		}
	}()

	<-t.Dying()
	s.log.Info("Closing server...")
	close(s.exiting)
	cancel()
	//wait for cancel end
	<-waitCh

	s.log.Info("Policy server exited.")
	return nil
}
