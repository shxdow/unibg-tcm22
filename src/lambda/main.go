package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shxdow/tcm/gare"
)

func main() {
	lambda.Start(gare.HandleRaceUploadRequest)
}
