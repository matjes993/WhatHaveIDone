#compdef whid
# WHID tab completion for zsh

_whid() {
    local -a commands sources

    commands=(
        'setup:Guided setup for a data source'
        'collect:Collect data from a source'
        'groom:Deduplicate, sort, and detect ghosts'
        'status:Show vault status overview'
        'update:Pull latest version from GitHub'
    )

    sources=(
        'gmail:Google Gmail'
    )

    case "$CURRENT" in
        2)
            _describe 'command' commands
            ;;
        3)
            case "${words[2]}" in
                setup|collect|groom)
                    _describe 'source' sources
                    ;;
            esac
            ;;
    esac
}

_whid "$@"
