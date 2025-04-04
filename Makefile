# Build configuration
CC ?= clang
ENABLE_ASAN ?= 0
BUILD_TYPE ?= debug
CFLAGS := -Wall -Wextra -pedantic -std=c11 -Iinclude
LDFLAGS :=
LIBS :=

# Directories
BIN_DIR := bin
TEST_DIR := test
INC_DIR := include
DOC_DIR := doc

# Sanitizer configuration
ifeq ($(ENABLE_ASAN),1)
  CFLAGS += -fsanitize=address
  LDFLAGS += -fsanitize=address
  export ASAN_OPTIONS=verbosity=0:detect_leaks=1:log_path=asan.log
endif

# Build type configuration
ifeq ($(BUILD_TYPE),release)
  CFLAGS += -O2 -DNDEBUG
else
  CFLAGS += -g -O0
endif

# Test and benchmark binaries
TEST_BINARY := $(BIN_DIR)/test_bptree
BENCH_BINARY := $(BIN_DIR)/bench_bptree
EXAMPLE_BINARY := $(BIN_DIR)/example

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show the targets and their descriptions
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; \
 	{printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

.PHONY: all
all: clean doc test bench example ## Build all targets and run tests and benchmarks

.PHONY: test
test: $(TEST_BINARY) ## Build and run tests
	@echo "Running tests..."
	./$(TEST_BINARY)

$(TEST_BINARY): $(TEST_DIR)/test_bptree.c $(INC_DIR)/bptree.h | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

.PHONY: bench
bench: $(BENCH_BINARY) ## Build and run benchmarks
	@echo "Running benchmarks..."
	./$(BENCH_BINARY)

$(BENCH_BINARY): $(TEST_DIR)/bench_bptree.c $(INC_DIR)/bptree.h | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

.PHONY: clean
clean: ## Remove build artifacts
	@echo "Cleaning up build artifacts..."
	rm -rf $(BIN_DIR) $(DOC_DIR) *.gcno *.gcda *.gcov

.PHONY: format
format: ## Format source code
	@echo "Formatting source code..."
	@command -v clang-format >/dev/null 2>&1 || { echo >&2 "clang-format not found."; exit 1; }
	clang-format -i $(TEST_DIR)/*.c $(INC_DIR)/*.h

.PHONY: lint
lint: ## Run linter checks
	@echo "Running linters..."
	cppcheck --enable=all --inconclusive --quiet --std=c11 -I$(INC_DIR) $(TEST_DIR)

.PHONY: install
install: ## Install header file
	@echo "Installing bptree.h system-wide..."
	install -d /usr/local/include
	install -m 0644 $(INC_DIR)/bptree.h /usr/local/include/

.PHONY: uninstall
uninstall: ## Remove installed header file
	@echo "Uninstalling bptree.h..."
	rm -f /usr/local/include/bptree.h

.PHONY: install-deps
install-deps: ## Install development dependencies (for Debian-based systems)
	@echo "Installing development dependencies..."
	sudo apt-get update && sudo apt-get install -y \
		gcc gdb clang clang-format cppcheck valgrind

.PHONY: coverage
coverage: CFLAGS += -fprofile-arcs -ftest-coverage
coverage: LDFLAGS += -lgcov
coverage: clean $(TEST_BINARY) ## Generate code coverage report
	@echo "Running tests for coverage..."
	./$(TEST_BINARY)
	@echo "Generating code coverage report..."
	gcov -o $(BIN_DIR) $(TEST_DIR)/test_bptree.c
	@echo "Coverage report generated"

.PHONY: memcheck
memcheck: $(EXAMPLE_BINARY) $(TEST_BINARY) $(BENCH_BINARY) ## Run memory checks with Valgrind
	@echo "Running memory checks..."
	@echo "Running example with Valgrind..."
	valgrind --leak-check=full --show-leak-kinds=all ./$(EXAMPLE_BINARY)
	@echo "Running tests with Valgrind..."
	valgrind --leak-check=full --show-leak-kinds=all ./$(TEST_BINARY)
	@echo "Running benchmarks with Valgrind..."
	valgrind --leak-check=full --show-leak-kinds=all ./$(BENCH_BINARY)
	@echo "Valgrind checks completed"

.PHONY: doc
doc: ## Generate documentation
	@echo "Generating documentation..."
	@test -f Doxyfile || { echo "Error: Doxyfile not found."; exit 1; }
	doxygen Doxyfile

.PHONY: example
example: $(EXAMPLE_BINARY) ## Run examples
	@echo "Running the example..."
	./$(EXAMPLE_BINARY)

$(EXAMPLE_BINARY): $(TEST_DIR)/example.c $(INC_DIR)/bptree.h | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

.PHONY: release
release: BUILD_TYPE=release
release: all ## Build everything in release mode (optimized)
	@echo "Building in release mode..."
	@$(MAKE) all
