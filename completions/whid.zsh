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
        'vectorize:Vectorize vault data for semantic search'
        'search:Semantic search across your data'
        'status:Show vault status overview'
        'update:Pull latest version from GitHub'
    )

    sources=(
        'gmail:Gmail inbox (API)'
        'contacts-google:Google Contacts (API)'
        'contacts-linkedin:LinkedIn export (CSV or full dir)'
        'contacts-facebook:Facebook export (JSON)'
        'contacts-instagram:Instagram export (JSON)'
        'books-goodreads:Goodreads library (CSV)'
        'books-audible:Audible library (CSV)'
        'youtube:YouTube history (Takeout JSON)'
        'music-spotify:Spotify streaming history (JSON)'
        'finance-paypal:PayPal transactions (CSV)'
        'finance-bank:Bank transactions (CSV)'
        'shopping-amazon:Amazon orders (CSV)'
        'notes:Markdown/text notes (directory)'
        'podcasts:Podcast history (DB or CSV)'
        'health:Apple Health (XML export)'
        'browser-chrome:Chrome browsing history (local)'
        'calendar:Google Calendar (API)'
        'calendar-ics:Calendar events (ICS file)'
        'maps:Google Maps timeline (Takeout JSON)'
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
                vectorize)
                    local -a vsources
                    vsources=(
                        'gmail:Vectorize Gmail'
                        'contacts:Vectorize Contacts'
                        'books:Vectorize Books'
                        'youtube:Vectorize YouTube'
                        'music:Vectorize Music'
                        'finance:Vectorize Finance'
                        'shopping:Vectorize Shopping'
                        'notes:Vectorize Notes'
                        'podcasts:Vectorize Podcasts'
                        'health:Vectorize Health'
                        'browser:Vectorize Browser'
                        'calendar:Vectorize Calendar'
                        'maps:Vectorize Maps'
                    )
                    _describe 'source' vsources
                    ;;
            esac
            ;;
    esac
}

_whid "$@"
