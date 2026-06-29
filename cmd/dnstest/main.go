package main

import (
	"fmt"
	"os"

	"github.com/miekg/dns"
)

func main() {
	server := "127.0.0.1:8553"
	domain := "dns-test.DEFAULT_GROUP.public.benbroo."

	// Test A records
	fmt.Printf("=== DNS A Record Query: %s ===\n", domain)
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(domain), dns.TypeA)

	c := new(dns.Client)
	r, _, err := c.Exchange(m, server)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	if len(r.Answer) == 0 {
		fmt.Println("No A records found")
	}
	for _, ans := range r.Answer {
		fmt.Printf("  A: %s\n", ans.(*dns.A).A.String())
	}

	// Test SRV records
	fmt.Printf("\n=== DNS SRV Record Query: %s ===\n", domain)
	m2 := new(dns.Msg)
	m2.SetQuestion(dns.Fqdn(domain), dns.TypeSRV)

	r2, _, err := c.Exchange(m2, server)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	if len(r2.Answer) == 0 {
		fmt.Println("No SRV records found")
	}
	for _, ans := range r2.Answer {
		srv := ans.(*dns.SRV)
		fmt.Printf("  SRV: target=%s port=%d weight=%d\n", srv.Target, srv.Port, srv.Weight)
	}
	for _, extra := range r2.Extra {
		if a, ok := extra.(*dns.A); ok {
			fmt.Printf("  EXTRA A: %s -> %s\n", a.Hdr.Name, a.A.String())
		}
	}

	// Test weighted resolution (multiple queries)
	fmt.Printf("\n=== Weighted DNS Resolution (5 queries) ===\n")
	for i := 0; i < 5; i++ {
		m3 := new(dns.Msg)
		m3.SetQuestion(dns.Fqdn(domain), dns.TypeA)
		r3, _, err := c.Exchange(m3, server)
		if err != nil {
			fmt.Printf("Query %d error: %v\n", i+1, err)
			continue
		}
		if len(r3.Answer) > 0 {
			first := r3.Answer[0].(*dns.A)
			fmt.Printf("  Query %d: first answer = %s\n", i+1, first.A.String())
		}
	}

	// Test non-existent domain
	fmt.Printf("\n=== Non-existent Domain ===\n")
	m4 := new(dns.Msg)
	m4.SetQuestion("nonexistent.DEFAULT_GROUP.public.benbroo.", dns.TypeA)
	r4, _, err := c.Exchange(m4, server)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("  Answers: %d (expected 0)\n", len(r4.Answer))
	}

	fmt.Println("\nDNS test complete!")
}
