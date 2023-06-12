package main

import (
	"context"
	"fmt"
	"github.com/Filecoin-Titan/titan-sdk-go"
	"github.com/Filecoin-Titan/titan-sdk-go/config"
	"github.com/cheggaaa/pb"
	"github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/pkg/errors"
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
	defer client.Close()

	cid := "QmXRrLjxgHd2Ls8jFZby2fx2wQuuqBkamQE8ibY6TnREA4"
	size, reader, err := client.GetFile(context.Background(), cid)
	if err != nil {
		log.Fatal(err)
	}

	defer reader.Close()

	bar := pb.New64(size).SetUnits(pb.U_BYTES)
	bar.ShowSpeed = true
	barR := bar.NewProxyReader(reader)

	bar.Start()
	filename := "download.car"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}

	io.Copy(file, barR)

	bar.Finish()
	fmt.Printf("CAR file save to %s\n", filename)

	outputPath := "download.mp4"
	if err = DecodeCARFile(filename, outputPath); err != nil {
		log.Fatal(err)
	}
}

func DecodeCARFile(CARFilePath, outputPath string) error {
	fmt.Printf("Decoding CAR file %s to %s ...\n", CARFilePath, outputPath)

	// Open the CAR file
	carFile, err := os.Open(CARFilePath)
	if err != nil {
		return errors.Errorf("failed to opening CAR file: %v", err)
	}
	defer carFile.Close()

	// Create a blockstore
	bs, err := blockstore.OpenReadOnly(CARFilePath)
	if err != nil {
		return errors.Errorf("failed to creating blockstore from CAR file: %v", err)
	}

	// Create a blockservice
	blockService := blockservice.New(bs, offline.Exchange(bs))

	// Create a merkledag service
	dagService := merkledag.NewDAGService(blockService)

	// Get the root CID of the CAR file
	rootsReader, err := carv2.NewReader(carFile)
	if err != nil {
		return errors.Errorf("failed to creating roots reader: %v", err)
	}
	rootCIDs, err := rootsReader.Roots()
	if err != nil {
		return errors.Errorf("failed to getting root CIDs: %v", err)
	}

	// Get the IPLD node from the root CID
	node, err := dagService.Get(context.Background(), rootCIDs[0])
	if err != nil {
		return errors.Errorf("failed to getting IPLD node from root CID: %v", err)
	}

	newFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}

	merkleNode, err := unixfile.NewUnixfsFile(context.Background(), dagService, node)
	if err != nil {
		return err
	}

	switch f := merkleNode.(type) {
	case files.File:
		_, err = io.Copy(newFile, f)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("file type %T is not supported", node)
	}
}
