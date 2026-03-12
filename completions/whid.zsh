#compdef whid
# WHID tab completion for zsh

_whid() {
    local -a commands sources

    commands=(
        'setup:Guided setup for a data source'
        'collect:Collect data from a source'
        'enrich:Backfill metadata from the API'
        'clean:RAG-optimized cleaning pass'
        'groom:Deduplicate, sort, and detect ghosts'
        'status:Show vault status overview'
        'update:Pull latest version from GitHub'
    )

    sources=(
        'gmail:Google Gmail'
        'contacts-google:Google Contacts'
        'contacts-linkedin:LinkedIn connections (CSV import)'
        'contacts-facebook:Facebook friends (JSON import)'
        'contacts-instagram:Instagram followers (JSON import)'
    )

    case "$CURRENT" in
        2)
            _describe 'command' commands
            ;;
        3)
            case "${words[2]}" in
                setup|collect|enrich|clean|groom)
                    _describe 'source' sources
                    ;;
            esac
            ;;
    esac
}

_whid "$@"
