package common

import (
	"fmt"

	"github.com/google/uuid"
)

func TempIntermediateDir(partitionID int) string {
	return fmt.Sprintf("temp/intermediate/partition-%d", partitionID)
}

func IntermediateDir(partitionID int) string {
	return fmt.Sprintf("out/intermediate/partition-%d", partitionID)
}

func TempFinalDir() string {
	return "temp/final"
}

func FinalDir() string {
	return "out/final"
}

func TempMapOutPath(partitionID int, mapID int, attemptID uuid.UUID) string {
	dir := TempIntermediateDir(partitionID)
	return fmt.Sprintf("%s/mapper-%s-%d", dir, attemptID, mapID)
}

func FinalMapOutPath(partitionID, mapID int) string {
	dir := IntermediateDir(partitionID)
	return fmt.Sprintf("%s/mapper-%d", dir, mapID)
}

func TempReduceOutPath(reduceID int, attemptID uuid.UUID) string {
	dir := TempFinalDir()
	return fmt.Sprintf("%s/%s-%d", dir, attemptID, reduceID)
}

func FinalReduceOutPath(reduceID int) string {
	dir := FinalDir()
	return fmt.Sprintf("%s/out-%d", dir, reduceID)
}
