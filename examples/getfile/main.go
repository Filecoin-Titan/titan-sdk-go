package main

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go"
	"github.com/Filecoin-Titan/titan-sdk-go/config"
	"github.com/cheggaaa/pb"
	"io"
	"log"
	"os"
)

func main() {
	address := os.Getenv("LOCATOR_API_INFO")

	client, err := titan.New(
		config.AddressOption(address),
		config.TraversalModeOption(config.TraversalModeRange),
	)
	if err != nil {
		log.Fatal(err)
	}

	cid := "QmZvRybasN8ihe8PkyhHhJ1YbR9j4YXQgRxtcdRr9PGpUm"
	size, reader, err := client.GetFile(context.Background(), cid)
	if err != nil {
		log.Fatal(err)
	}

	defer reader.Close()

	bar := pb.New64(size).SetUnits(pb.U_BYTES)
	bar.ShowSpeed = true
	barR := bar.NewProxyReader(reader)

	bar.Start()
	defer bar.Finish()

	io.Copy(io.Discard, barR)
}
