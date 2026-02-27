#!/bin/sh
# Entrypoint script for supervisord that sets environment variables
# for controlling which celery workers to start

# Fetch API keys from Platform Keys Service if URL is configured
if [ -n "$PLATFORM_KEYS_URL" ] && [ -n "$INTERNAL_SERVICE_SECRET" ]; then
    echo "Fetching API keys from Platform Keys Service..."
    KEYS_RESPONSE=$(curl -sf "$PLATFORM_KEYS_URL" \
        -H "X-Internal-Service-Secret: $INTERNAL_SERVICE_SECRET" \
        2>/dev/null)

    if [ $? -eq 0 ] && [ -n "$KEYS_RESPONSE" ]; then
        # Extract keys using jq if available, otherwise try basic parsing
        if command -v jq >/dev/null 2>&1; then
            ANTHROPIC_KEY=$(echo "$KEYS_RESPONSE" | jq -r '.keys.anthropic // empty')
            OPENAI_KEY=$(echo "$KEYS_RESPONSE" | jq -r '.keys.openai // empty')
        else
            # Fallback: simple grep/sed parsing
            ANTHROPIC_KEY=$(echo "$KEYS_RESPONSE" | grep -o '"anthropic":"[^"]*"' | sed 's/"anthropic":"//;s/"$//')
            OPENAI_KEY=$(echo "$KEYS_RESPONSE" | grep -o '"openai":"[^"]*"' | sed 's/"openai":"//;s/"$//')
        fi

        # Set API keys if retrieved and not already set with real values
        if [ -n "$ANTHROPIC_KEY" ]; then
            if [ -z "$ANTHROPIC_API_KEY" ] || [ "$ANTHROPIC_API_KEY" = "your-anthropic-api-key-here" ]; then
                export ANTHROPIC_API_KEY="$ANTHROPIC_KEY"
                export ANTHROPIC_DEFAULT_API_KEY="$ANTHROPIC_KEY"
                echo "  Anthropic API key loaded from Platform Keys Service"
            fi
        fi

        if [ -n "$OPENAI_KEY" ]; then
            if [ -z "$OPENAI_API_KEY" ] || [ "$OPENAI_API_KEY" = "your-openai-api-key-here" ]; then
                export OPENAI_API_KEY="$OPENAI_KEY"
                export OPENAI_DEFAULT_API_KEY="$OPENAI_KEY"
                echo "  OpenAI API key loaded from Platform Keys Service"
            fi
        fi
    else
        echo "  Warning: Could not fetch keys from Platform Keys Service"
    fi
fi

# Default to lightweight mode if not set
if [ -z "$USE_LIGHTWEIGHT_BACKGROUND_WORKER" ]; then
    export USE_LIGHTWEIGHT_BACKGROUND_WORKER="true"
fi

# Set the complementary variable for supervisord
# because it doesn't support %(not ENV_USE_LIGHTWEIGHT_BACKGROUND_WORKER) syntax
if [ "$USE_LIGHTWEIGHT_BACKGROUND_WORKER" = "true" ]; then
    export USE_SEPARATE_BACKGROUND_WORKERS="false"
else
    export USE_SEPARATE_BACKGROUND_WORKERS="true"
fi

echo "Worker mode configuration:"
echo "  USE_LIGHTWEIGHT_BACKGROUND_WORKER=$USE_LIGHTWEIGHT_BACKGROUND_WORKER"
echo "  USE_SEPARATE_BACKGROUND_WORKERS=$USE_SEPARATE_BACKGROUND_WORKERS"

# Launch supervisord with environment variables available
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
