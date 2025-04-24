package net

import (
	"fmt"
	inet "net"
	"strconv"
)

func GetLocalIP() (string, error) {
	ifaces, err := inet.Interfaces()
	if err != nil {
		return "", fmt.Errorf("failed to get network interfaces: %w", err)
	}
	for _, iface := range ifaces {
		// Skip interfaces that are down or loopback
		if iface.Flags&inet.FlagUp == 0 || iface.Flags&inet.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip inet.IP
			switch v := addr.(type) {
			case *inet.IPNet:
				ip = v.IP
			case *inet.IPAddr:
				ip = v.IP
			}
			// Skip loopback and IPv6
			if ip == nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("no IP found")
}

func IsValidIP(ip string) bool {
	return inet.ParseIP(ip) != nil
}

func IsValidPort(port string) bool {
	p, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	if p < 0 || p > 65535 {
		return false
	}
	return true
}

func IsPortInUse(port int) bool {
	listener, err := inet.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return true
	}
	_ = listener.Close()
	return false
}
