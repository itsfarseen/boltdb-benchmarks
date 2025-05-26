package main

// vi:ts=2:

import (
	. "boltdb_benchmarks/strategy"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

// Benchmark results struct
type BenchmarkResult struct {
	Strategy     string
	Bulk         bool
	Operation    string
	Duration     time.Duration
	StorageBytes int64
	RecordCount  int
}

// Generate test data
func generateUser(id int64) *UserInfo {
	rand.Seed(id)
	return &UserInfo{
		ID:          id,
		Username:    fmt.Sprintf("user_%d", id),
		Email:       fmt.Sprintf("user%d@example.com", id),
		FirstName:   fmt.Sprintf("First_%d", id),
		LastName:    fmt.Sprintf("Last_%d", id),
		Age:         int32(rand.Intn(60) + 18),
		Height:      float32(150 + rand.Intn(50)),
		Weight:      float32(50 + rand.Intn(100)),
		Balance:     rand.Float64() * 10000,
		IsActive:    rand.Intn(2) == 1,
		CreatedAt:   time.Now().Unix() - int64(rand.Intn(365*24*3600)),
		UpdatedAt:   time.Now().Unix(),
		LoginCount:  int32(rand.Intn(1000)),
		Score:       rand.Float64() * 100,
		Description: fmt.Sprintf("This is a description for user %d with some random text to make it longer and more realistic.", id),
	}
}

func generateUsers(recordCount int) []*UserInfo {
	users := make([]*UserInfo, recordCount)
	for i := range recordCount {
		users[i] = generateUser(int64(i))
	}
	return users
}

// Get database file size
func getDBSize(dbPath string) (int64, error) {
	info, err := os.Stat(dbPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Run benchmark for a specific strategy
func runBenchmark(
	strategy *StrategyVariant,
	users []*UserInfo,
	readIDs []int64,
	updateIDs []int64,
	runs int,
) []BenchmarkResult {
	recordCount := len(users)
	var results []BenchmarkResult

	for run := range runs {
		// create & open temp DB
		dbPath := fmt.Sprintf("/tmp/bench_%s_%d_%d.db", strategy.Name(), recordCount, run)
		defer os.Remove(dbPath)
		db, err := bbolt.Open(dbPath, 0600, nil)
		if err != nil {
			log.Fatal(err)
		}

		// SETUP & WRITE ALL
		strategy.Setup(db)
		t0 := time.Now()
		strategy.WriteAll(db, users)
		writeTotal := time.Since(t0)
		db.Close()
		storageSize, _ := getDBSize(dbPath)

		// REOPEN for reads & updates
		db, _ = bbolt.Open(dbPath, 0600, nil)

		// 1) many single reads
		t0 = time.Now()
		for _, id := range readIDs {
			if _, err := strategy.Read(db, id); err != nil {
				log.Printf("Read error: %v", err)
			}
		}
		readTotal := time.Since(t0)

		// 2) ReadMany (one batch)
		t0 = time.Now()
		batch, err := strategy.ReadMany(db, readIDs[0], len(readIDs))
		readManyTotal := time.Since(t0)
		if err != nil {
			log.Printf("ReadMany error: %v", err)
		}

		// 3) field sum over all
		t0 = time.Now()
		if _, err := strategy.ReadFieldSum(db, "balance", recordCount); err != nil {
			log.Printf("FieldSum error: %v", err)
		}
		fieldSumTotal := time.Since(t0)

		// 4) many single updates
		t0 = time.Now()
		for _, id := range updateIDs {
			if err := strategy.UpdateField(db, id, "balance", 12345.67); err != nil {
				log.Printf("Update error: %v", err)
			}
		}
		updateTotal := time.Since(t0)

		db.Close()

		// now normalize: divide by count of ops
		perWrite := writeTotal / time.Duration(recordCount)
		perRead := readTotal / time.Duration(len(readIDs))
		perReadMany := readManyTotal / time.Duration(len(batch))
		perFieldSum := fieldSumTotal / time.Duration(recordCount)
		perUpdate := updateTotal / time.Duration(len(updateIDs))

		base := BenchmarkResult{
			Strategy:     strategy.Name(),
			Bulk:         strategy.Bulk,
			StorageBytes: storageSize,
			RecordCount:  recordCount,
		}

		results = append(results,
			BenchmarkResult{base.Strategy, base.Bulk, "Write", perWrite, base.StorageBytes, base.RecordCount},
			BenchmarkResult{base.Strategy, base.Bulk, "Read", perRead, base.StorageBytes, base.RecordCount},
			BenchmarkResult{base.Strategy, base.Bulk, "ReadMany", perReadMany, base.StorageBytes, base.RecordCount},
			BenchmarkResult{base.Strategy, base.Bulk, "FieldSum", perFieldSum, base.StorageBytes, base.RecordCount},
			BenchmarkResult{base.Strategy, base.Bulk, "Update", perUpdate, base.StorageBytes, base.RecordCount},
		)
	}
	return results
}

func calculateAverages(results []BenchmarkResult) []BenchmarkResult {
	type key struct {
		strat string
		bulk  bool
		op    string
		rc    int
	}
	grouped := make(map[key][]BenchmarkResult)
	for _, r := range results {
		k := key{r.Strategy, r.Bulk, r.Operation, r.RecordCount}
		grouped[k] = append(grouped[k], r)
	}

	var avgResults []BenchmarkResult
	for k, slice := range grouped {
		var sumDur time.Duration
		var sumBytes int64
		for _, r := range slice {
			sumDur += r.Duration
			sumBytes += r.StorageBytes
		}
		n := time.Duration(len(slice))
		avgResults = append(avgResults, BenchmarkResult{
			Strategy:     k.strat,
			Bulk:         k.bulk,
			Operation:    k.op,
			Duration:     sumDur / n,
			StorageBytes: sumBytes / int64(len(slice)),
			RecordCount:  k.rc,
		})
	}
	return avgResults
}

// Print results
func printResults(results []BenchmarkResult) {
	// First, sort so that grouping by RecordCount is stable:
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.RecordCount != b.RecordCount {
			return a.RecordCount < b.RecordCount
		}
		if a.Strategy != b.Strategy {
			return a.Strategy < b.Strategy
		}
		if a.Bulk != b.Bulk {
			return !a.Bulk && b.Bulk
		}
		return a.Operation < b.Operation
	})

	// Group by record count
	byCount := make(map[int][]BenchmarkResult)
	var counts []int
	for _, r := range results {
		if _, seen := byCount[r.RecordCount]; !seen {
			counts = append(counts, r.RecordCount)
		}
		byCount[r.RecordCount] = append(byCount[r.RecordCount], r)
	}
	sort.Ints(counts)

	// For each slice, print its own table
	for _, rc := range counts {
		subset := byCount[rc]
		fmt.Printf("\n--- %d Records ---\n", rc)
		fmt.Printf(
			"%-15s %-8s %-10s %-10s %-10s %-10s %-10s %-12s\n",
			"Strategy", "Insert", "Write(μs)", "Read(μs)",
			"FldSum(μs)", "Update(μs)", "ReadMany(μs)", "Storage(KB)",
		)
		fmt.Println(strings.Repeat("-", 15+8+10*5+12))

		// Build op → result map for each (strategy, bulk)
		type key struct {
			strat string
			bulk  bool
		}
		table := make(map[key]map[string]BenchmarkResult)
		for _, r := range subset {
			k := key{r.Strategy, r.Bulk}
			if table[k] == nil {
				table[k] = make(map[string]BenchmarkResult)
			}
			table[k][r.Operation] = r
		}

		// Dedup keys and sort
		var variants []key
		for k := range table {
			variants = append(variants, k)
		}
		sort.Slice(variants, func(i, j int) bool {
			a, b := variants[i], variants[j]
			if a.strat != b.strat {
				return a.strat < b.strat
			}
			return !a.bulk && b.bulk
		})

		for _, v := range variants {
			ops := table[v]
			toUs := func(d time.Duration) float64 {
				return float64(d.Nanoseconds()) / 1e3
			}
			write := toUs(ops["Write"].Duration)
			read := toUs(ops["Read"].Duration)
			fs := toUs(ops["FieldSum"].Duration)
			up := toUs(ops["Update"].Duration)
			many := toUs(ops["ReadMany"].Duration)
			sizeKB := float64(ops["Write"].StorageBytes) / 1024.0

			insertMode := "Single"
			if v.bulk {
				insertMode = "Bulk"
			}
			fmt.Printf(
				"%-15s %-8s %-10.2f %-10.2f %-10.2f %-10.2f %-10.2f %-12.2f\n",
				v.strat, insertMode, write, read, fs, up, many, sizeKB,
			)
		}
	}
}

// Write CSV of all results
func writeCSV(path string, results []BenchmarkResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header
	w.Write([]string{
		"Strategy", "Insert", "RecordCount",
		"Operation", "Duration_us", "StorageBytes",
	})

	for _, r := range results {
		insertMode := "Single"
		if r.Bulk {
			insertMode = "Bulk"
		}
		rec := []string{
			r.Strategy,
			insertMode,
			strconv.Itoa(r.RecordCount),
			r.Operation,
			fmt.Sprintf("%.0f", float64(r.Duration.Nanoseconds())/1e3),
			strconv.FormatInt(r.StorageBytes, 10),
		}
		w.Write(rec)
	}
	return w.Error()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("BBolt Storage Strategy Benchmark")
	fmt.Println("=================================")

	baseStrategies := []StorageStrategy{
		&JSONStrategy{},
		&GOBStrategy{},
		&BinaryStrategy{},
		&BinaryWithNamesStrategy{},
		&MultiKVStrategy{},
		&NestedBucketStrategy{},
	}

	// Create variants for individual and bulk writes
	var strategies []*StrategyVariant
	for _, base := range baseStrategies {
		strategies = append(strategies, &StrategyVariant{Strategy: base, Bulk: false})
		strategies = append(strategies, &StrategyVariant{Strategy: base, Bulk: true})

	}
	recordCounts := []int{10, 100, 1_000, 10_000, 25_000, 50_000, 75_000, 100_000, 250_000, 500_000, 750_000, 1_000_000}
	maxCount := recordCounts[len(recordCounts)-1]
	allUsers := generateUsers(maxCount)

	benchmarkRuns := 10

	var allResults []BenchmarkResult

	for _, rc := range recordCounts {
		subset := allUsers[:rc]
		readIDs_ := rand.Perm(rc)[:rc/2]
		readIDs := make([]int64, len(readIDs_))
		for i, v := range readIDs_ {
			readIDs[i] = int64(v)
		}

		updateIDs_ := rand.Perm(rc)[:rc/2]
		updateIDs := make([]int64, len(readIDs_))
		for i, v := range updateIDs_ {
			updateIDs[i] = int64(v)
		}

		for _, strat := range strategies {
			fmt.Printf("Benchmarking %s (bulk=%v) with %d records...\n",
				strat.Strategy.Name(), strat.Bulk, rc)
			res := runBenchmark(strat, subset, readIDs, updateIDs, benchmarkRuns)
			allResults = append(allResults, res...)
		}
	}

	// Calculate and print averages
	averages := calculateAverages(allResults)
	printResults(averages)
	if err := writeCSV("benchmark_results.csv", averages); err != nil {
		log.Fatalf("failed to write CSV: %v", err)
	}
	fmt.Println("\nWrote CSV: benchmark_results.csv")
}
