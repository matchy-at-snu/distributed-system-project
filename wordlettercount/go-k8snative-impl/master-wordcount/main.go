package main

import (
	"cloud.google.com/go/storage"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/net/context"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	// out of cluster configuration
	//var kubeconfig *string
	//if home := homedir.HomeDir(); home != "" {
	//	kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	//} else {
	//	kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	//}

	inputStrPtr := flag.String("input", "",
		"The input directory in the form of /path/to/output, must be provided")
	outputStrPtr := flag.String("output", "",
		"The output directory in the form of /path/to/output, default to stdout")
	chunkSizePtr := flag.Int("chunkSize", 1024,
		"The maximum chunk size (in KB) which will be fed to the wc-mapper, default to 1024KB")

	flag.Parse()

	// Build config out of Kubeconfig
	// fuck you Go
	//if *kubeconfig != "" {
	//	config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	//} else {
	log.Println("Get in-cluster k8s configuration...")
	config, err := rest.InClusterConfig()
	//}

	if err != nil {
		log.Fatal(err)
	}

	// Create clientset
	log.Println("Create clientset...")
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 创建用于部署的 deploymentsClient ，可以指定命名空间（以字符串形式喂入）
	log.Println("Create stateful")
	statefulSetClient := clientset.AppsV1().StatefulSets("wordlettercount")

	input := *inputStrPtr
	output := *outputStrPtr
	chunkSize := (*chunkSizePtr) * 1024

	if input == "" {
		log.Fatal("Must provide input directory!")
	}

	// Get input file from Google Cloud Storage Bucket
	data, err := downloadFile(input)
	if err != nil {
		log.Fatal(err)
	}

	// Split data []byte into chunks of chunkSize and store in chunks [][]byte
	chunks := split(data, chunkSize)

	log.Println("Create mappers...")
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		log.Println("Get the latest version of Statefulset...")
		result, getErr := statefulSetClient.Get(
			context.TODO(),
			"wc-mapper",
			metav1.GetOptions{})
		if getErr != nil {
			log.Fatal(fmt.Errorf("Failed to get latest version of Statefulset: %v", getErr))
		}

		result.Spec.Replicas = int32Ptr(int32(len(chunks)))
		log.Printf("Set mapper replicas to : %v\n", *(result.Spec.Replicas))
		_, updateErr := statefulSetClient.Update(context.TODO(), result, metav1.UpdateOptions{})
		return updateErr
	})
	if retryErr != nil {
		log.Fatal(fmt.Errorf("Update failed %v", retryErr))
	}

	for {
		time.Sleep(5 * time.Second)
		result, getErr := statefulSetClient.Get(
			context.TODO(),
			"wc-mapper",
			metav1.GetOptions{})
		if getErr != nil {
			log.Fatal(fmt.Errorf("Failed to get latest version of Statefulset: %v", getErr))
		}
		log.Printf("Updating mappers: expect %v, have %v\n", *result.Spec.Replicas, result.Status.ReadyReplicas)
		if *result.Spec.Replicas == result.Status.ReadyReplicas {
			break
		}
	}

	// Delete wc-mapper deployment deletion
	//defer func() {
	//	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
	//		result, getErr := statefulSetClient.Get(
	//			context.TODO(),
	//			"wc-mapper",
	//			metav1.GetOptions{})
	//		if getErr != nil {
	//			log.Fatal(fmt.Errorf("Failed to get latest version of Statefulset: %v", getErr))
	//		}
	//
	//		result.Spec.Replicas = int32Ptr(0)
	//		_, updateErr := statefulSetClient.Update(context.TODO(), result, metav1.UpdateOptions{})
	//		return updateErr
	//	})
	//	if retryErr != nil {
	//		log.Fatal(fmt.Errorf("Update failed %v", retryErr))
	//	}
	//}()

	// Open reducers
	log.Println("Create reducers...")
	retryErr = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, getErr := statefulSetClient.Get(
			context.TODO(),
			"reducers",
			metav1.GetOptions{})
		if getErr != nil {
			log.Fatal(fmt.Errorf("Failed to get latest version of Statefulset: %v", getErr))
		}

		result.Spec.Replicas = int32Ptr(5)
		log.Printf("Set reducer replicas to : %v\n", *(result.Spec.Replicas))
		_, updateErr := statefulSetClient.Update(context.TODO(), result, metav1.UpdateOptions{})
		return updateErr
	})
	if retryErr != nil {
		log.Fatal(fmt.Errorf("Update failed %v", retryErr))
	}

	// Delete reducer deployment deletion
	//defer func() {
	//	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
	//		result, getErr := statefulSetClient.Get(
	//			context.TODO(),
	//			"reducers",
	//			metav1.GetOptions{})
	//		if getErr != nil {
	//			log.Fatal(fmt.Errorf("Failed to get latest version of Statefulset: %v", getErr))
	//		}
	//
	//		result.Spec.Replicas = int32Ptr(0)
	//		_, updateErr := statefulSetClient.Update(context.TODO(), result, metav1.UpdateOptions{})
	//		return updateErr
	//	})
	//	if retryErr != nil {
	//		log.Fatal(fmt.Errorf("Update failed %v", retryErr))
	//	}
	//}()

	// when they are ready send GET to them to retrieve message
	// {IP : {word : count}}
	var client = &http.Client{}

	mapperHost := os.Getenv("MAPPER_HOST")
	mapperIPs, err := net.LookupIP(mapperHost)
	if err != nil {
		log.Fatal(err)
	}

	if len(chunks) != len(mapperIPs) {
		log.Fatal("Some pods did not start correctly!")
	}

	var mapIPChunk = map[string]string{}
	for i := 0; i < len(mapperIPs); i++ {
		mapIPChunk[mapperIPs[i].String()] = string(chunks[i])
	}

	// IP: {word: count, word: count ...}
	var mapResult = map[string]map[string]int{}

	log.Println("Start mapping...")

	var wgm sync.WaitGroup
	wgm.Add(len(mapperIPs))

	for host, chunk := range mapIPChunk {
		go func(host string, chunk string) {
			defer wgm.Done()
			req, _ := http.NewRequest("GET", fmt.Sprintf(
				"http://%s:%s/map", host, os.Getenv("MAPPER_PORT")), nil)

			q := req.URL.Query()
			q.Add("str", chunk)
			req.URL.RawQuery = q.Encode()

			var (
				res     *http.Response
				retries int = 3
				reqErr  error
			)
			for retries > 0 {
				res, reqErr = client.Do(req)
				if reqErr != nil {
					log.Println(reqErr)
					retries -= 1
				} else {
					break
				}
			}

			if res != nil {
				defer res.Body.Close()

				//
				//body, readErr := ioutil.ReadAll(res.Body)
				//if readErr != nil {
				//	log.Fatal(readErr)
				//}
				//
				//buf := bytes.NewBuffer(body)
				//
				var decodedMap map[string]int
				decError := json.NewDecoder(res.Body).Decode(&decodedMap)
				if decError != nil {
					log.Fatal("decode error: ", decError)
				}

				//
				//decoder := gob.NewDecoder(buf)
				//decodeErr := decoder.Decode(&decodeMap)
				//if decodeErr != nil {
				//	log.Fatal("decode error:", decodeErr)
				//}

				mapResult[host] = decodedMap
			} else {
				log.Fatal("Didn't get anything, WTF?")
			}

		}(host, chunk)
	}

	wgm.Wait()

	log.Println("Mapping finished!")

	for ip, host := range mapResult {
		log.Println(ip)
		for word, count := range host {
			log.Println(word, count)
			break
		}
		break
	}

	log.Println("Start shuffling...")

	// Shuffle
	var shuffleResult = map[string][]int{}
	for _, host := range mapResult {
		for word, count := range host {
			shuffleResult[word] = append(shuffleResult[word], count)
		}
	}

	log.Println("Shuffling finished!")

	// It opens really slow, hahah

	for {
		time.Sleep(1 * time.Second)
		result, getErr := statefulSetClient.Get(
			context.TODO(),
			"reducers",
			metav1.GetOptions{})
		if getErr != nil {
			log.Fatal(fmt.Errorf("Failed to get latest version of Statefulset: %v", getErr))
		}
		log.Printf("Updating reducers: expect %v, have %v\n", *result.Spec.Replicas, result.Status.ReadyReplicas)
		if *result.Spec.Replicas == result.Status.ReadyReplicas {
			break
		}
	}

	// Send to reducer
	reducerHost := os.Getenv("REDUCER_HOST")

	reducerIPs, err := net.LookupIP(reducerHost)
	if len(reducerIPs) != 5 {
		log.Fatal("Some reducers didn't start normally")
	}
	if err != nil {
		log.Fatal(err)
	}

	var words []string
	for word := range shuffleResult {
		words = append(words, word)
	}

	reduceChunkSize := int(math.Ceil(float64(len(words)) / 5.0))

	// { IP : {words : [count, count, count, ... ]}}
	mapIPWords := map[string]map[string][]int{}

	for idx, reducerIP := range reducerIPs {
		if idx*reduceChunkSize >= len(words) {
			break
		}
		var start = idx * reduceChunkSize
		reduceWords := words[start:min(start, len(words))]

		mapIPWords[reducerIP.String()] = map[string][]int{}
		for _, reduceKey := range reduceWords {
			mapIPWords[reducerIP.String()][reduceKey] = shuffleResult[reduceKey]
		}
	}

	log.Println("Start reducing...")

	var wgr sync.WaitGroup
	wgr.Add(5)

	// IP : {word: count}
	var reduceResult = map[string]map[string]int{}

	for host, words := range mapIPWords {
		go func(host string, words map[string][]int) {
			defer wgr.Done()
			req, _ := http.NewRequest("GET", fmt.Sprintf(
				"http://%s:%s/reduce", host, os.Getenv("REDUCER_PORT")), nil)
			//buf := new(bytes.Buffer)

			//encoder := gob.NewEncoder(buf)
			//_ = encoder.Encode(words)

			b, encError := json.Marshal(words)
			if encError != nil {
				log.Fatal("encode error:", encError)
			}

			q := req.URL.Query()
			q.Add("body", string(b))
			req.URL.RawQuery = q.Encode()

			res, reqError := client.Do(req)
			if reqError != nil {
				log.Fatal("reducer http request error: ", reqError)
			}
			//body, _ := ioutil.ReadAll(res.Body)
			//_ = res.Body.Close()
			//
			//b = bytes.NewBuffer(body)

			var decodedReduce = map[string]int{}
			decError := json.NewDecoder(res.Body).Decode(&decodedReduce)
			if decError != nil {
				log.Fatal(decError)
			}

			//decoder := gob.NewDecoder(buf)
			//_ = decoder.Decode(&decodedReduce)

			reduceResult[host] = decodedReduce
		}(host, words)
	}

	wgr.Wait()

	log.Println("Reducing finished!")

	kvp := tokvList(reduceResult)

	log.Println("Sorting results...")
	sort.Sort(sort.Reverse(kvp))

	// Write output to Stdout or designated output file
	if *outputStrPtr == "" {
		log.Println("Use std out")
	} else {
		log.Printf("Use output path to print output! Check it at gs://%s\n", output)
		if err := uploadFile(output, kvp.String()); err != nil {
			log.Fatal(err)
		}
	}
}

type kvPair struct {
	Key   string
	Value int
}

type kvList []kvPair

func (p kvList) Len() int { return len(p) }

func (p kvList) Less(i, j int) bool {
	if p[i].Value != p[j].Value {
		return p[i].Value < p[j].Value
	} else {
		return p[i].Key < p[j].Key
	}
}

func (p kvList) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p kvList) String() string {
	n := len(p)
	perc5lim := int(math.Ceil(float64(n) * 0.05))
	popular := p[:perc5lim]
	common := p[n/2-perc5lim : n/2+perc5lim]
	rare := p[n-perc5lim:]
	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)

	table.SetHeader([]string{"CATEGORY", "RANK", "WORD", "FREQUENCY"})
	for i, e := range popular {
		table.Append([]string{
			"POPULAR", strconv.Itoa(i), e.Key, string(rune(e.Value)),
		})
	}
	for i, e := range common {
		table.Append([]string{
			"COMMON", string(rune(i + n/2 - perc5lim)), e.Key, string(rune(e.Value)),
		})
	}
	for i, e := range rare {
		table.Append([]string{
			"RARE", string(rune(n - perc5lim + i)), e.Key, string(rune(e.Value)),
		})
	}

	table.Render()
	return tableString.String()
}
func tokvList(input map[string]map[string]int) kvList {
	var res kvList
	for _, kvMap := range input {
		for k, v := range kvMap {
			res = append(res, kvPair{k, v})
		}
	}
	return res
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// downloadFile downloads an object from bucket matchy-bucket
func downloadFile(object string) ([]byte, error) {
	bucket := "matchy-bucket"
	// object := "object-name"
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	log.Printf("Blob %v downloaded.\n", object)
	return data, nil
}

// uploadFile uploads an object with data.
func uploadFile(object string, data string) error {
	bucket := "matchy-bucket"
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = wc.Write([]byte(data)); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}
	log.Printf("Blob %v uploaded.\n", object)
	return nil
}

// split splits a byte array into chunks no larger than chunkSize
func split(data []byte, chunkSize int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, (len(data)/chunkSize)+1)

	for len(data) >= chunkSize {
		chunk, data = data[:chunkSize], data[chunkSize:]
		chunks = append(chunks, chunk)
	}
	if len(data) > 0 {
		chunks = append(chunks, data[:])
	}
	return chunks
}

func int32Ptr(i int32) *int32 { return &i }
