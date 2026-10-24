#!/usr/bin/env bash
# Checks the Ukrainian translation of the documentation against the English
# source: README.md and docs/en/*.md are translated in README.uk.md and
# docs/uk/*.md (docs/uk/GLOSSARY.md has no English counterpart). The files
# directly under docs/ only point to their new place under docs/en/.
#
# Each translation starts with a comment naming its source and the commit the
# translation was made from:
#
#   <!-- translated-from: docs/en/labels.md @ <commit> -->
#
# Errors (exit status 1): an English file without a translation, a translation
# without an English file, a missing or wrong comment, a commit that is not in
# the history, a missing language switcher. A warning: the English file has
# changed since that commit, so the translation lags behind. After updating the
# translation, set the commit to one that has the current English file, such as
# the commit of the English change.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

errors=0
warnings=0

# report LEVEL FILE MESSAGE prints a problem, as a GitHub annotation in CI.
report() {
	local level=$1 file=$2 msg=$3
	if [ "${GITHUB_ACTIONS:-}" = true ]; then
		echo "::${level} file=${file}::${msg}"
	else
		echo "${level}: ${file}: ${msg}"
	fi
	if [ "$level" = error ]; then
		errors=$((errors + 1))
	else
		warnings=$((warnings + 1))
	fi
}

# translation_of prints the Ukrainian counterpart of an English file.
translation_of() {
	case $1 in
	README.md) echo README.uk.md ;;
	docs/en/*) echo "docs/uk/${1#docs/en/}" ;;
	esac
}

# source_of prints the English counterpart of a Ukrainian file.
source_of() {
	case $1 in
	README.uk.md) echo README.md ;;
	docs/uk/*) echo "docs/en/${1#docs/uk/}" ;;
	esac
}

# has_switcher FILE TARGET succeeds if one of the first lines of FILE links to
# TARGET, the file in the other language.
has_switcher() {
	head -n 5 "$1" | grep -qF "]($2)"
}

english=(README.md)
for f in docs/en/*.md; do
	if [ -f "$f" ]; then
		english+=("$f")
	fi
done
ukrainian=(README.uk.md)
for f in docs/uk/*.md; do
	if [ -f "$f" ] && [ "$f" != docs/uk/GLOSSARY.md ]; then
		ukrainian+=("$f")
	fi
done

for src in "${english[@]}"; do
	uk=$(translation_of "$src")
	if [ ! -f "$uk" ]; then
		report error "$src" "no Ukrainian translation: create $uk"
		continue
	fi
	if [ "$src" = README.md ]; then
		link=README.uk.md
	else
		link=../uk/${src#docs/en/}
	fi
	has_switcher "$src" "$link" || report error "$src" "no language switcher linking to $link at the top"
done

shallow=$(git rev-parse --is-shallow-repository)
for uk in "${ukrainian[@]}"; do
	[ -f "$uk" ] || continue
	src=$(source_of "$uk")
	if [ ! -f "$src" ]; then
		report error "$uk" "no English source: $src does not exist"
		continue
	fi
	if [ "$uk" = README.uk.md ]; then
		link=README.md
	else
		link=../en/${src#docs/en/}
	fi
	has_switcher "$uk" "$link" || report error "$uk" "no language switcher linking to $link at the top"

	header=$(head -n 3 "$uk" | sed -n 's/^<!-- translated-from: \([^ ]*\) @ \([0-9a-f]\{7,40\}\) -->$/\1 \2/p' | head -n 1)
	if [ -z "$header" ]; then
		report error "$uk" "no '<!-- translated-from: $src @ <commit> -->' comment at the top"
		continue
	fi
	named=${header% *}
	commit=${header#* }
	if [ "$named" != "$src" ]; then
		report error "$uk" "translated-from names $named, expected $src"
		continue
	fi
	if ! git rev-parse --quiet --verify "${commit}^{commit}" >/dev/null; then
		hint=""
		if [ "$shallow" = true ]; then
			hint=" (the clone is shallow; fetch the full history)"
		fi
		report error "$uk" "translated-from commit $commit is not in the history${hint}"
		continue
	fi
	# Compared with the working tree, so an uncommitted change counts too.
	if ! git diff --quiet "$commit" -- "$src"; then
		report warning "$uk" "translation lags behind: $src changed since ${commit:0:12}; see git diff ${commit:0:12} -- $src"
	fi
done

echo "docs translations: ${#english[@]} English files, ${errors} errors, ${warnings} warnings"
[ "$errors" -eq 0 ]
