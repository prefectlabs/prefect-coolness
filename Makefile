.DEFAULT_GOAL := install

.bookkeeping/development.txt: .bookkeeping/uv requirements.txt
	mkdir -p .bookkeeping
	cat requirements.txt > .bookkeeping/development.txt.next

	uv pip sync .bookkeeping/development.txt.next

	mv .bookkeeping/development.txt.next .bookkeeping/development.txt

.bookkeeping/uv:
	mkdir -p .bookkeeping
	touch .bookkeeping/uv.next

	pip install -U pip uv

	mv .bookkeeping/uv.next .bookkeeping/uv

%.txt: %.in .bookkeeping/uv
	uv pip compile --upgrade --overrides overrides.txt --output-file $@ $<

.PHONY: install
install: .bookkeeping/development.txt

.PHONY: clean
clean:
	rm -Rf .bookkeeping/
