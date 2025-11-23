package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var (
	apiURL string
	apiKey string
	output string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "djs",
		Short: "Distributed Job Scheduler CLI",
		Long:  "Command-line interface for managing distributed tasks and workflows",
	}

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&apiURL, "api-url", "u", "http://localhost:8001", "Scheduler API URL")
	rootCmd.PersistentFlags().StringVarP(&apiKey, "api-key", "k", "", "API key for authentication")
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "table", "Output format (table, json, yaml)")

	// Task commands
	taskCmd := &cobra.Command{
		Use:   "task",
		Short: "Manage tasks",
	}

	taskCmd.AddCommand(
		&cobra.Command{
			Use:   "create",
			Short: "Create a new task",
			RunE:  createTask,
		},
		&cobra.Command{
			Use:   "list",
			Short: "List tasks",
			RunE:  listTasks,
		},
		&cobra.Command{
			Use:   "get [task-id]",
			Short: "Get task details",
			Args:  cobra.ExactArgs(1),
			RunE:  getTask,
		},
		&cobra.Command{
			Use:   "cancel [task-id]",
			Short: "Cancel a task",
			Args:  cobra.ExactArgs(1),
			RunE:  cancelTask,
		},
	)

	// Workflow commands
	workflowCmd := &cobra.Command{
		Use:   "workflow",
		Short: "Manage workflows",
	}

	workflowCmd.AddCommand(
		&cobra.Command{
			Use:   "create",
			Short: "Create a workflow from file",
			RunE:  createWorkflow,
		},
		&cobra.Command{
			Use:   "list",
			Short: "List workflows",
			RunE:  listWorkflows,
		},
		&cobra.Command{
			Use:   "get [workflow-id]",
			Short: "Get workflow details",
			Args:  cobra.ExactArgs(1),
			RunE:  getWorkflow,
		},
	)

	// Worker commands
	workerCmd := &cobra.Command{
		Use:   "worker",
		Short: "Manage workers",
	}

	workerCmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List workers",
			RunE:  listWorkers,
		},
	)

	// Stats command
	statsCmd := &cobra.Command{
		Use:   "stats",
		Short: "Show statistics",
		RunE:  showStats,
	}

	// Cluster command
	clusterCmd := &cobra.Command{
		Use:   "cluster",
		Short: "Show cluster information",
		RunE:  showCluster,
	}

	rootCmd.AddCommand(taskCmd, workflowCmd, workerCmd, statsCmd, clusterCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func createTask(cmd *cobra.Command, args []string) error {
	name, _ := cmd.Flags().GetString("name")
	taskType, _ := cmd.Flags().GetString("type")
	priority, _ := cmd.Flags().GetInt("priority")

	payload := map[string]interface{}{
		"name":     name,
		"type":     taskType,
		"priority": priority,
		"payload":  map[string]interface{}{},
	}

	data, _ := json.Marshal(payload)
	resp, err := makeRequest("POST", "/api/v1/tasks", bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	fmt.Printf("Task created: %s\n", string(resp))
	return nil
}

func listTasks(cmd *cobra.Command, args []string) error {
	resp, err := makeRequest("GET", "/api/v1/tasks", nil)
	if err != nil {
		return err
	}

	if output == "json" {
		fmt.Println(string(resp))
		return nil
	}

	var result map[string]interface{}
	json.Unmarshal(resp, &result)

	tasks, ok := result["tasks"].([]interface{})
	if !ok {
		fmt.Println("No tasks found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tPRIORITY\tCREATED")

	for _, t := range tasks {
		task := t.(map[string]interface{})
		id := task["id"].(string)
		if len(id) > 12 {
			id = id[:12] + "..."
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.0f\t%s\n",
			id,
			task["name"],
			task["type"],
			task["status"],
			task["priority"],
			formatTime(task["created_at"].(string)),
		)
	}

	w.Flush()
	return nil
}

func getTask(cmd *cobra.Command, args []string) error {
	taskID := args[0]
	resp, err := makeRequest("GET", fmt.Sprintf("/api/v1/tasks/%s", taskID), nil)
	if err != nil {
		return err
	}

	if output == "json" {
		fmt.Println(string(resp))
		return nil
	}

	var task map[string]interface{}
	json.Unmarshal(resp, &task)

	fmt.Printf("Task ID: %s\n", task["id"])
	fmt.Printf("Name: %s\n", task["name"])
	fmt.Printf("Type: %s\n", task["type"])
	fmt.Printf("Status: %s\n", task["status"])
	fmt.Printf("Priority: %.0f\n", task["priority"])
	fmt.Printf("Created: %s\n", formatTime(task["created_at"].(string)))

	return nil
}

func cancelTask(cmd *cobra.Command, args []string) error {
	taskID := args[0]
	_, err := makeRequest("DELETE", fmt.Sprintf("/api/v1/tasks/%s", taskID), nil)
	if err != nil {
		return err
	}

	fmt.Printf("Task %s cancelled\n", taskID)
	return nil
}

func createWorkflow(cmd *cobra.Command, args []string) error {
	// Implementation for workflow creation from file
	fmt.Println("Workflow creation not yet implemented")
	return nil
}

func listWorkflows(cmd *cobra.Command, args []string) error {
	resp, err := makeRequest("GET", "/api/v1/workflows", nil)
	if err != nil {
		return err
	}

	fmt.Println(string(resp))
	return nil
}

func getWorkflow(cmd *cobra.Command, args []string) error {
	workflowID := args[0]
	resp, err := makeRequest("GET", fmt.Sprintf("/api/v1/workflows/%s", workflowID), nil)
	if err != nil {
		return err
	}

	fmt.Println(string(resp))
	return nil
}

func listWorkers(cmd *cobra.Command, args []string) error {
	resp, err := makeRequest("GET", "/api/v1/workers", nil)
	if err != nil {
		return err
	}

	if output == "json" {
		fmt.Println(string(resp))
		return nil
	}

	var result map[string]interface{}
	json.Unmarshal(resp, &result)

	workers, ok := result["workers"].([]interface{})
	if !ok {
		fmt.Println("No workers found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATUS\tTASKS\tHEALTH\tLAST HEARTBEAT")

	for _, wk := range workers {
		worker := wk.(map[string]interface{})

		fmt.Fprintf(w, "%s\t%s\t%.0f/%.0f\t%.1f\t%s\n",
			worker["id"],
			worker["status"],
			worker["current_tasks"],
			worker["max_concurrency"],
			worker["health_score"],
			formatTime(worker["last_heartbeat"].(string)),
		)
	}

	w.Flush()
	return nil
}

func showStats(cmd *cobra.Command, args []string) error {
	resp, err := makeRequest("GET", "/api/v1/stats", nil)
	if err != nil {
		return err
	}

	if output == "json" {
		fmt.Println(string(resp))
		return nil
	}

	var stats map[string]interface{}
	json.Unmarshal(resp, &stats)

	fmt.Printf("📊 Scheduler Statistics\n\n")
	fmt.Printf("Total Tasks: %.0f\n", stats["total"])
	fmt.Printf("Success Rate: %.2f%%\n", stats["success_rate"])

	if byStatus, ok := stats["by_status"].(map[string]interface{}); ok {
		fmt.Printf("\nBy Status:\n")
		for status, count := range byStatus {
			fmt.Printf("  %s: %.0f\n", status, count)
		}
	}

	return nil
}

func showCluster(cmd *cobra.Command, args []string) error {
	resp, err := makeRequest("GET", "/api/v1/cluster", nil)
	if err != nil {
		return err
	}

	var cluster map[string]interface{}
	json.Unmarshal(resp, &cluster)

	fmt.Printf("🌐 Cluster Information\n\n")
	fmt.Printf("Node ID: %s\n", cluster["node_id"])
	fmt.Printf("Is Leader: %v\n", cluster["is_leader"])
	fmt.Printf("State: %s\n", cluster["state"])

	return nil
}

func makeRequest(method, path string, body io.Reader) ([]byte, error) {
	url := apiURL + path

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "ApiKey "+apiKey)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(data))
	}

	return data, nil
}

func formatTime(timeStr string) string {
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return timeStr
	}
	return t.Format("2006-01-02 15:04:05")
}
