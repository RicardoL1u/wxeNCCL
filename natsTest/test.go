package main

import (
	"fmt"
	"log"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
)

func main() {
	// Create an operator key pair (private key)
	okp, err := nkeys.CreateOperator()
	if err != nil {
		log.Fatal(err)
	}
	// Extract the public key
	opk, err := okp.PublicKey()
	if err != nil {
		log.Fatal(err)
	}

	// Create an operator claim using the public key for the identifier
	oc := jwt.NewOperatorClaims(opk)
	oc.Name = "OperatorName"

	// Add an operator signing key to sign accounts
	oskp, err := nkeys.CreateOperator()
	if err != nil {
		log.Fatal(err)
	}
	// Get the public key for the signing key
	ospk, err := oskp.PublicKey()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("Operator JWT:", oc)
	// Add the signing key to the operator - this makes any account
	// issued by the signing key to be valid for the operator
	oc.SigningKeys.Add(ospk)

	// Self-sign the operator JWT - the operator trusts itself
	operatorJWT, err := oc.Encode(okp)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Operator JWT:", operatorJWT)

	// Create an account keypair
	akp, err := nkeys.CreateAccount()
	if err != nil {
		log.Fatal(err)
	}
	// Extract the public key for the account
	apk, err := akp.PublicKey()
	if err != nil {
		log.Fatal(err)
	}
	// Create the claim for the account using the public key of the account
	ac := jwt.NewAccountClaims(apk)
	ac.Name = "AccountName"

	// Create a signing key that we can use for issuing users
	askp, err := nkeys.CreateAccount()
	if err != nil {
		log.Fatal(err)
	}
	// Extract the public key
	aspk, err := askp.PublicKey()
	if err != nil {
		log.Fatal(err)
	}
	// Add the signing key (public) to the account
	ac.SigningKeys.Add(aspk)
	fmt.Print("Account JWT:", ac)
	// Now we could encode an issue the account using the operator
	// key that we generated above, but this will illustrate that
	// the account could be self-signed, and given to the operator
	// who can then re-sign it
	accountJWT, err := ac.Encode(akp)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Account JWT:", accountJWT)

	// The operator would decode the provided token, if the token
	// is not self-signed or signed by an operator or tampered with
	// the decoding would fail
	decodedAC, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Decoded Account:", decodedAC)
}
