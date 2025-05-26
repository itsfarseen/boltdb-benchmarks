.PHONY: benchmark analyze

benchmark:
	go run app/main.go

analyze:
	python3 analyze.py
