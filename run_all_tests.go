package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	fmt.Println("🚀 Running fluxdl Test Suite")
	fmt.Println("=" + strings.Repeat("=", 50))

	startTime := time.Now()

	// Test categories to run
	testCategories := []struct {
		name        string
		path        string
		description string
	}{
		{
			name:        "Unit Tests",
			path:        "./tests/unit/...",
			description: "All unit tests (KV, Queue, Stream)",
		},
		{
			name:        "Integration Tests",
			path:        "./tests/integration/...",
			description: "Integration tests",
		},
		{
			name:        "Comprehensive Tests",
			path:        "./tests/comprehensive_streams_test.go",
			description: "Comprehensive streams & pub/sub demonstrations",
		},
	}

	totalTests := 0
	passedTests := 0
	failedTests := 0
	var failedCategories []string

	for _, category := range testCategories {
		fmt.Printf("\n📋 Running %s\n", category.name)
		fmt.Printf("   %s\n", category.description)
		fmt.Println("   " + strings.Repeat("-", 40))

		// Run the tests
		cmd := exec.Command("go", "test", "-v", category.path)
		cmd.Dir = "."

		output, err := cmd.CombinedOutput()
		outputStr := string(output)

		// Parse results
		lines := strings.Split(outputStr, "\n")
		categoryPassed := 0
		categoryFailed := 0
		categoryTotal := 0

		for _, line := range lines {
			if strings.Contains(line, "--- PASS:") {
				categoryPassed++
				categoryTotal++
			} else if strings.Contains(line, "--- FAIL:") {
				categoryFailed++
				categoryTotal++
			}
		}

		// Update totals
		totalTests += categoryTotal
		passedTests += categoryPassed
		failedTests += categoryFailed

		// Print category results
		if err != nil || categoryFailed > 0 {
			fmt.Printf("   ❌ FAILED: %d passed, %d failed, %d total\n", categoryPassed, categoryFailed, categoryTotal)
			failedCategories = append(failedCategories, category.name)

			// Show failed test details
			if categoryFailed > 0 {
				fmt.Println("   Failed tests:")
				for _, line := range lines {
					if strings.Contains(line, "--- FAIL:") {
						testName := strings.TrimSpace(strings.Split(line, "--- FAIL:")[1])
						fmt.Printf("     • %s\n", testName)
					}
				}
			}
		} else {
			fmt.Printf("   ✅ PASSED: %d tests\n", categoryPassed)
		}

		// Show any compilation errors
		if err != nil && !strings.Contains(outputStr, "FAIL") {
			fmt.Printf("   ⚠️  Error: %v\n", err)
			if strings.Contains(outputStr, "no Go files") {
				fmt.Printf("   ℹ️  No test files found in %s\n", category.path)
			} else {
				fmt.Printf("   Output: %s\n", outputStr)
			}
		}
	}

	// Final summary
	duration := time.Since(startTime)
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📊 TEST SUMMARY")
	fmt.Println(strings.Repeat("=", 60))

	if failedTests == 0 {
		fmt.Printf("🎉 ALL TESTS PASSED!\n")
		fmt.Printf("✅ %d tests passed in %v\n", passedTests, duration.Round(time.Millisecond))
	} else {
		fmt.Printf("❌ SOME TESTS FAILED\n")
		fmt.Printf("✅ Passed: %d\n", passedTests)
		fmt.Printf("❌ Failed: %d\n", failedTests)
		fmt.Printf("📊 Total:  %d\n", totalTests)
		fmt.Printf("⏱️  Duration: %v\n", duration.Round(time.Millisecond))

		if len(failedCategories) > 0 {
			fmt.Printf("\nFailed categories:\n")
			for _, cat := range failedCategories {
				fmt.Printf("  • %s\n", cat)
			}
		}
	}

	// Additional information
	fmt.Printf("\n💡 Test Infrastructure:\n")
	fmt.Printf("   • Tests use automatic server management\n")
	fmt.Printf("   • Each test gets its own isolated server instance\n")
	fmt.Printf("   • Temporary data directories are cleaned up automatically\n")

	// Exit with appropriate code
	if failedTests > 0 {
		os.Exit(1)
	}
}
