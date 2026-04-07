# Load required libraries
library(ggplot2)
library(dplyr)
library(tikzDevice)

# Get command line arguments
args <- commandArgs(trailingOnly = TRUE)

# Check if two arguments are provided
if (length(args) != 2) {
  stop("Usage: Rscript plot_timestamps.R <file1.csv> <file2.csv>")
}

file1 <- args[1]
file2 <- args[2]

# Read both CSV files
# First column: count (y-axis)
# Second column: year (x-axis)
data1 <- read.csv(file1)
data2 <- read.csv(file2)

# Ensure the column names are 'count' and 'year' for consistency
colnames(data1) <- c("count", "year")
colnames(data2) <- c("count", "year")

# Add source labels to distinguish the datasets
data1$source <- basename(file1)
data2$source <- basename(file2)

# Combine the datasets
combined_data <- rbind(data1, data2)

# Sort by year
combined_data <- combined_data %>%
  arrange(year)

# Plot: Line plot showing both datasets
line_plot <- ggplot(combined_data, aes(x = year, y = count, color = source, group = source)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  scale_color_manual(values = c("blue", "red")) +
  labs(
    title = "Commits per Year Comparison",
    x = "Year",
    y = "Number of Commits",
    color = "Dataset"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(hjust = 0.5),
    legend.position = "bottom"
  )

# Display the plot
print(line_plot)

# Save plots in PNG format
ggsave("WoC_commitTimestamps_comparison_plot.png", plot = line_plot, width = 10, height = 6, dpi = 300)

# Save plots in PDF format
ggsave("WoC_commitTimestamps_comparison_plot.pdf", plot = line_plot, width = 10, height = 6)

# Save plots in TikZ format using tikzDevice
tikz("WoC_commitTimestamps_comparison_plot.tex", width = 10, height = 6)
print(line_plot)
dev.off()

# Print summary statistics for each dataset
cat("\nSummary Statistics:\n")
cat("\nDataset 1 (", basename(file1), "):\n", sep = "")
cat("  Total commits:", sum(data1$count), "\n")
cat("  Total years:", nrow(data1), "\n")
cat("  Average commits per year:", mean(data1$count), "\n")
cat("  Median commits per year:", median(data1$count), "\n")

cat("\nDataset 2 (", basename(file2), "):\n", sep = "")
cat("  Total commits:", sum(data2$count), "\n")
cat("  Total years:", nrow(data2), "\n")
cat("  Average commits per year:", mean(data2$count), "\n")
cat("  Median commits per year:", median(data2$count), "\n")

cat("\nOutput files created:\n")
cat("- WoC_commitTimestamps_comparison_plot.png\n")
cat("- WoC_commitTimestamps_comparison_plot.pdf\n")
cat("- WoC_commitTimestamps_comparison_plot.tex\n")
