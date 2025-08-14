package common

import (
	"fmt"

	"github.com/google/uuid"
)

func IntermediateDir(partitionID int) string {
	return fmt.Sprintf("intermediate/partition-%d", partitionID)
}

func FinalDir() string {
	return "final"
}

func TempMapOutPath(partitionID int, mapID int, attemptID uuid.UUID) string {
	dir := IntermediateDir(partitionID)
	return fmt.Sprintf("%s/mapper-%s-%d", dir, attemptID, mapID)
}

func FinalMapOutPath(partitionID, mapID int) string {
	dir := IntermediateDir(partitionID)
	return fmt.Sprintf("%s/mapper-%d", dir, mapID)
}

func TempReduceOutPath(reduceID int, attemptID uuid.UUID) string {
	dir := FinalDir()
	return fmt.Sprintf("%s/%s-%d", dir, attemptID, reduceID)
}

func FinalReduceOutPath(reduceID int) string {
	dir := FinalDir()
	return fmt.Sprintf("%s/out-%d", dir, reduceID)
}
