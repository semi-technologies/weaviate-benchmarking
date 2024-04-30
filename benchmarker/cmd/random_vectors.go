package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"unsafe"

	"github.com/spf13/cobra"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	"google.golang.org/protobuf/proto"
)

func initRandomVectors() {
	rootCmd.AddCommand(randomVectorsCmd)
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Queries,
		"queries", "q", 100, "Set the number of queries the benchmarker should run")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Dimensions,
		"dimensions", "d", 768, "Set the vector dimensions (must match your data)")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.WhereFilter,
		"where", "w", "", "An entire where filter as a string")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	randomVectorsCmd.PersistentFlags().StringVar(&globalConfig.DB,
		"db", "weaviate", "The tool you're benchmarking")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "graphql", "The API to use on benchmarks")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "http://localhost:8080", "The origin that Weaviate is running at")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
}

var randomVectorsCmd = &cobra.Command{
	Use:   "random-vectors",
	Short: "Benchmark nearVector searches",
	Long:  `Benchmark random nearVector searches`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "random-vectors"

		if err := cfg.Validate(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if len(cfg.WhereFilter) > 0 {
			filter := fmt.Sprintf(", where: { %s }", cfg.WhereFilter)
			cfg.WhereFilter = strings.Replace(filter, "\"", "\\\"", -1)
		}

		if cfg.DB == "weaviate" {

			var w io.Writer
			if cfg.OutputFile == "" {
				w = os.Stdout
			} else {
				f, err := os.Create(cfg.OutputFile)
				if err != nil {
					fatal(err)
				}

				defer f.Close()
				w = f

			}

			result := benchmarkNearVector(cfg)
			if cfg.OutputFormat == "json" {
				result.WriteJSONTo(w)
			} else if cfg.OutputFormat == "text" {
				result.WriteTextTo(w)
			}

			if cfg.OutputFile != "" {
				infof("results succesfully written to %q", cfg.OutputFile)
			}
			return
		}

		fmt.Printf("unrecognized db\n")
		os.Exit(1)
	},
}

func randomVector(dims int) []float32 {
	vector := []float32{}

	for i := 0; i < dims; i++ {
		vector = append(vector, rand.Float32()*2-1)
	}

	return vector
}

func nearVectorQueryJSONGraphQLRaw(raw string) []byte {
	return []byte(fmt.Sprintf(`{
"query": "%s"
}`, raw))
}

func nearVectorQueryJSONGraphQL(className string, vec []float32, limit int, whereFilter string) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
"query": "{ Get { %s(limit: %d, nearVector: {vector:%s}%s) { _additional { id } } } }"
}`, className, limit, string(vecJSON), whereFilter))
}

func nearVectorQueryJSONRest(className string, vec []float32, limit int) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
		"nearVector":{"vector":%s},
		"limit":%d
}`, string(vecJSON), limit))
}

func encodeUnsafe(fs []float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&fs[0])), len(fs)*4)
}

func nearVectorQueryGrpc(className string, vec []float32, limit int, tenant string) []byte {

	searchRequest := &weaviategrpc.SearchRequest{
		Collection: className,
		Limit:      uint32(limit),
		NearVector: &weaviategrpc.NearVector{
			VectorBytes: encodeUnsafe(vec),
		},
		Metadata: &weaviategrpc.MetadataRequest{
			Certainty: false,
			Distance:  false,
			Uuid:      true,
		},
	}

	if tenant != "" {
		searchRequest.Tenant = tenant
	}

	data, err := proto.Marshal(searchRequest)
	if err != nil {
		fmt.Printf("grpc marshal err: %v\n", err)
	}

	return data
}

func benchmarkNearVector(cfg Config) Results {
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		if cfg.API == "graphql" {
			return QueryWithNeighbors{
				Query: nearVectorQueryJSONGraphQL(cfg.ClassName, randomVector(cfg.Dimensions), cfg.Limit, cfg.WhereFilter),
			}
		}

		if cfg.API == "rest" {
			return QueryWithNeighbors{
				Query: nearVectorQueryJSONRest(cfg.ClassName, randomVector(cfg.Dimensions), cfg.Limit),
			}
		}

		if cfg.API == "grpc" {
			return QueryWithNeighbors{
				Query: nearVectorQueryGrpc(cfg.ClassName, randomVector(cfg.Dimensions), cfg.Limit, cfg.Tenant),
			}
		}

		return QueryWithNeighbors{}
	})
}
