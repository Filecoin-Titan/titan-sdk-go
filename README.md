# Titan SDK for go

The Titan SDK is a Go-based software development kit (SDK) that provides developers with a set of tools for interacting with the Titan network. 
The SDK can be used to download data from the Titan network and verify its authenticity, as well as to submit proofs of work for specific tasks.


## Installation

To use the titan SDK, you'll first need to install Go and set up a Go development environment. Once you have Go installed and configured, you can install the titan SDK using Go modules:
```bash
go get "github.com/Filecoin-Titan/titan-sdk-go"
```

## Configuring
In the titan SDK Go, you can configure settings for service clients. Most settings are optional; however, for each service client, you must specify a titan `address` and your `token`. The SDK uses these values to send requests to the correct titan address and sign requests with the correct token.

## Examples

Here's an example of how to use the SDK interface to download a file:

```
package main

import (
	"context"
	"fmt"
	"io"
	"time"
	"github.com/Filecoin-Titan/titan-sdk-go"
	"github.com/Filecoin-Titan/titan-sdk-go/config"
)

func main() {
	address := "your_address_value_here"
	client, err := titan.New(
		config.AddressOption(address),
		config.TraversalModeOption(config.TraversalModeRange),
	if err != nil {
		log.Fatal(err)
	}	
	
	cid := "QmQmbAk3PRdgLPwUDbrDdbiqP23VCVrF1Y5MVYrBXwGZHy"
	_, reader, err := client.GetFile(context.Background(), cid)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	io.Copy(io.Discard, reader)
}

```

For more examples of how to use the Titan SDK, check out the examples directory in this repository. There, you'll find sample code snippets that demonstrate how to use the SDK interface to perform various tasks.

## Issues
Feel free to submit issues and enhancement requests.


## License

See [MIT](LICENSE) for more information.

