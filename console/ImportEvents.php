<?php

namespace Pensoft\Eventsfields\Console;

use Illuminate\Console\Command;
use Db;
use Carbon\Carbon;
use Log;
use System\Models\File;

class ImportEvents extends Command
{
    /**
     * @var string The console command name.
     */
    protected $name = 'events:import';

    /**
     * @var string The console command description.
     */
    protected $description = 'Import events from external API';

    /**
     * @var string The console command signature.
     */
    protected $signature = 'events:import
                            {--url= : API URL to fetch JSON from (overrides config)}
                            {--dry-run : Preview import without saving}
                            {--populate-missing : Populate missing data for existing entries}
                            {--update-matching : Update entries where title, start and end dates match}
                            {--update-all-matching : Update ALL fields for entries matching by title and dates}';

    /**
     * @var string Table name
     */
    protected $table = 'christophheich_calendar_entries';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        // Get URL from option, settings, or fail
        $url = $this->option('url');

        if (!$url) {
            $settings = \Pensoft\Eventsfields\Models\Settings::instance();
            $url = $settings->api_url;
        }

        if (empty($url)) {
            $this->error('No API URL configured. Set it in Settings or use --url option.');
            return 1;
        }

        // Check if import is enabled (skip check if running manually with --url)
        if (!$this->option('url')) {
            $settings = \Pensoft\Eventsfields\Models\Settings::instance();
            if (!$settings->import_enabled) {
                $this->warn('Automatic import is disabled in settings.');
                return 0;
            }
        }
        $dryRun = $this->option('dry-run');
        $populateMissing = $this->option('populate-missing');
        $updateMatching = $this->option('update-matching');
        $updateAllMatching = $this->option('update-all-matching');

        // If populate-missing mode, run that instead of normal import
        if ($populateMissing) {
            return $this->handlePopulateMissing($url, $dryRun);
        }

        // If update-matching mode, run that instead of normal import
        if ($updateMatching) {
            return $this->handleUpdateMatching($url, $dryRun);
        }

        // If update-all-matching mode, run that instead of normal import
        if ($updateAllMatching) {
            return $this->handleUpdateAllMatching($url, $dryRun);
        }

        $this->info("Starting events import...");
        $this->info("API URL: {$url}");

        // Fetch JSON from API
        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 60,
                    'header' => "Accept: application/json\r\n"
                ]
            ]);

            $response = file_get_contents($url, false, $context);

            if ($response === false) {
                $this->error('Failed to fetch data from API');
                Log::error('Events Import: Failed to fetch data from API', ['url' => $url]);
                return 1;
            }
        } catch (\Exception $e) {
            $this->error('API request failed: ' . $e->getMessage());
            Log::error('Events Import: API request failed', ['error' => $e->getMessage()]);
            return 1;
        }

        $data = json_decode($response, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error('Invalid JSON response: ' . json_last_error_msg());
            Log::error('Events Import: Invalid JSON', ['error' => json_last_error_msg()]);
            return 1;
        }

        if (!isset($data['items']) || !is_array($data['items'])) {
            $this->error('JSON does not contain items array');
            return 1;
        }

        $items = $data['items'];
        $totalItems = count($items);
        $this->info("Found {$totalItems} items in API response");

        // Pre-process: Group items by global_id and collect all dates
        $groupedItems = $this->groupItemsByGlobalId($items);
        $this->info("Found " . count($groupedItems) . " unique events after grouping");


        $stats = [
            'inserted' => 0,
            'updated' => 0,
            'skipped' => 0,
            'duplicates' => 0,
            'errors' => 0
        ];

        $processedIdentifiers = [];

        $progressBar = $this->output->createProgressBar(count($groupedItems));
        $progressBar->start();

        foreach ($groupedItems as $uniqueId => $item) {
            $progressBar->advance();

            try {
                // Skip items without required fields
                if (empty($item['global_id']) || empty($item['title'])) {
                    $stats['skipped']++;
                    continue;
                }

                // Already using global_id as unique identifier
                $uniqueId = $item['global_id'];

                // Skip if already processed in this import
                if (in_array($uniqueId, $processedIdentifiers)) {
                    $stats['skipped']++;
                    continue;
                }

                $processedIdentifiers[] = $uniqueId;

                // Transform item to entry
                $entry = $this->transformItem($item, $uniqueId);

                // Check for duplicate based on title and dates (ignoring time)
                if ($this->isDuplicate($entry['title'], $entry['start'], $entry['end'])) {
                    $stats['duplicates']++;
                    continue;
                }

                if ($dryRun) {
                    $stats['inserted']++;
                    continue;
                }

                // Check if exists by identifier (for updates) - include soft-deleted
                $existing = Db::table($this->table)
                    ->where('identifier', $uniqueId)
                    ->first();

                if ($existing) {
                    // Update
                    $entry['updated_at'] = Carbon::now();
                    $entry['deleted_at'] = null;
                    Db::table($this->table)
                        ->where('identifier', $uniqueId)
                        ->update($entry);
                    $stats['updated']++;

                    // Upload cover image if available and not already set
                    $this->uploadCoverImage($existing->id, $item, false);

                    // Attach default category if not already attached
                    $this->attachDefaultCategory($existing->id);
                } else {
                    // Insert
                    $entry['created_at'] = Carbon::now();
                    $entry['updated_at'] = Carbon::now();
                    $entry['deleted_at'] = null;
                    $entryId = Db::table($this->table)->insertGetId($entry);
                    $stats['inserted']++;

                    // Upload cover image if available
                    $this->uploadCoverImage($entryId, $item, true);

                    // Attach default category
                    $this->attachDefaultCategory($entryId);
                }

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing item: ' . ($item['global_id'] ?? 'unknown'));
                $this->error('Message: ' . $e->getMessage());
                $this->error('File: ' . $e->getFile() . ':' . $e->getLine());
                Log::error('Events Import: Error processing item', [
                    'global_id' => $item['global_id'] ?? 'unknown',
                    'error' => $e->getMessage(),
                    'file' => $e->getFile(),
                    'line' => $e->getLine()
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

        // Summary
        $this->info("Import completed!");
        $this->table(
            ['Metric', 'Count'],
            [
                ['Inserted', $stats['inserted']],
                ['Updated', $stats['updated']],
                ['Skipped', $stats['skipped']],
                ['Duplicates', $stats['duplicates']],
                ['Errors', $stats['errors']],
                ['Unique events', count($processedIdentifiers)],
            ]
        );

        // Log summary
        Log::info('Events Import completed', $stats);

        return 0;
    }

    /**
     * Check if an event with the same title and dates already exists
     * Compares dates only (ignores time component)
     */
    protected function isDuplicate(string $title, ?string $start, ?string $end): bool
    {
        $query = Db::table($this->table)
            ->where('title', $title);

        // Compare start date (date only, ignore time)
        if ($start) {
            $startDate = Carbon::parse($start)->format('Y-m-d');
            $query->whereRaw('DATE("start") = ?', [$startDate]);
        } else {
            $query->whereNull('start');
        }

        // Compare end date (date only, ignore time)
        // Note: "end" is a reserved keyword in PostgreSQL, must be quoted
        if ($end) {
            $endDate = Carbon::parse($end)->format('Y-m-d');
            $query->whereRaw('DATE("end") = ?', [$endDate]);
        } else {
            $query->whereNull('end');
        }

        return $query->exists();
    }

    /**
     * Transform API item to database entry
     */
    protected function transformItem(array $item, string $uniqueId): array
    {
        // Extract description (prefer HTML for formatting)
        $description = $this->extractText($item['texts'] ?? [], 'details', 'text/html');
        if (empty($description)) {
            $description = $this->extractText($item['texts'] ?? [], 'details', 'text/plain');
            // Convert plain text line breaks to HTML
            if ($description) {
                $description = nl2br($description);
            }
        }

        // Clean up unicode escaped HTML entities in description
        if ($description) {
            $description = $this->cleanUnicodeEscapes($description);
            $description = $this->cleanHtml($description);
        }

        // Extract teaser for meta description (plain text for SEO)
        $teaser = $this->extractText($item['texts'] ?? [], 'teaser', 'text/plain');
        if (empty($teaser)) {
            $teaser = $this->extractText($item['texts'] ?? [], 'teaser', 'text/html');
            $teaser = $this->stripHtml($teaser);
        }

        // Clean up unicode escaped HTML entities in teaser
        if ($teaser) {
            $teaser = $this->cleanUnicodeEscapes($teaser);
        }

        // Use pre-computed dates from groupItemsByGlobalId (earliest start, latest end)
        $start = $item['_computed_start'] ?? null;
        $end = $item['_computed_end'] ?? null;

        // Fallback: if no pre-computed dates, try to get from current item
        if (!$start && !empty($item['timeIntervals'])) {
            $interval = $item['timeIntervals'][0];
            $start = $this->parseDateTime($interval['start'] ?? null);
            $end = $this->parseDateTime($interval['end'] ?? null);
        }

        // Determine if all day event
        $allDay = false;
        if ($start && $end) {
            $startTime = Carbon::parse($start)->format('H:i:s');
            $endTime = Carbon::parse($end)->format('H:i:s');
            $allDay = ($startTime === '00:00:00' && $endTime === '23:59:59');
        }

        // Extract URL
        $url = $item['web'] ?? null;
        foreach ($item['media_objects'] ?? [] as $media) {
            if ($media['rel'] === 'venuewebsite' && !empty($media['url'])) {
                $url = $media['url'];
                break;
            }
        }

        // Extract organizer info
        $organizer = null;
        $organizerEmail = null;
        $organizerPhone = null;
        foreach ($item['addresses'] ?? [] as $address) {
            if (($address['rel'] ?? '') === 'organizer') {
                $organizer = $address['name'] ?? null;
                $organizerEmail = $address['email'] ?? null;
                $organizerPhone = $address['phone'] ?? null;
                break;
            }
        }

        // Categories/themes - map to allowed thematic focuses
        $categories = $item['categories'] ?? [];
        $keywords = $item['keywords'] ?? [];
        $mappedThemes = $this->mapThematicFocuses($categories, $keywords);
        $theme = !empty($mappedThemes) ? implode(', ', $mappedThemes) : null;

        // Keywords/tags
        $tags = !empty($keywords) ? implode(', ', $keywords) : null;

        // Target groups from features and keywords - map to allowed values
        $features = $item['features'] ?? [];
        $targetGroups = $this->mapTargetGroups($features, $keywords);
        $target = !empty($targetGroups) ? implode(', ', $targetGroups) : null;

        // Fee/price info
        $fee = $this->extractText($item['texts'] ?? [], 'PRICE_INFO', 'text/plain');

        // Build place
        $place = $this->buildPlace($item);

        // Generate slug
        $slug = $this->generateUniqueSlug($item['title'], $uniqueId);

        // Map country to country_id
        $countryId = $this->getCountryId($item['country'] ?? null);

        return [
            'title' => mb_substr($item['title'] ?? '', 0, 255),
            'slug' => $slug,
            'identifier' => $uniqueId,
            'start' => $start,
            'end' => $end,
            'all_day' => $allDay,
            'description' => $description,
            'url' => $url ? mb_substr($url, 0, 255) : null,
            'place' => $place ? mb_substr($place, 0, 255) : null,
            'country_id' => $countryId,
            'institution' => $organizer ? mb_substr($organizer, 0, 255) : null,
            'contact' => $organizerPhone ?? $item['phone'] ?? null,
            'email' => $organizerEmail ?? $item['email'] ?? null,
            'theme' => $theme ? mb_substr($theme, 0, 255) : null,
            'format' => $item['type'] ? mb_substr($item['type'], 0, 255) : null,
            'target' => $target ? mb_substr($target, 0, 255) : null,
            'tags' => $tags ? mb_substr($tags, 0, 255) : null,
            'fee' => $fee ? mb_substr($fee, 0, 255) : null,
            'meta_title' => mb_substr($item['title'] ?? '', 0, 255),
            'meta_description' => $teaser ? mb_substr($teaser, 0, 255) : null,
            'meta_keywords' => $tags ? mb_substr($tags, 0, 255) : null,
            'is_public' => true,
            'is_internal' => false,
            'show_on_timeline' => false,
            'source' => $item['source']['value'] ?? 'destination.one',
        ];
    }

    /**
     * Group items by global_id and collect all dates from all occurrences
     */
    protected function groupItemsByGlobalId(array $items): array
    {
        $grouped = [];

        foreach ($items as $item) {
            $globalId = $item['global_id'] ?? null;

            if (empty($globalId)) {
                continue;
            }

            if (!isset($grouped[$globalId])) {
                // First occurrence - use as base item
                $grouped[$globalId] = $item;
                $grouped[$globalId]['_all_dates'] = [];
            }

            // Collect dates from this occurrence

            // From attributes (interval_start/interval_end)
            $intervalStart = $this->getAttributeValue($item['attributes'] ?? [], 'interval_start');
            $intervalEnd = $this->getAttributeValue($item['attributes'] ?? [], 'interval_end');

            if ($intervalStart) {
                $grouped[$globalId]['_all_dates'][] = [
                    'start' => $intervalStart,
                    'end' => $intervalEnd
                ];
            }

            // From timeIntervals
            foreach ($item['timeIntervals'] ?? [] as $interval) {
                if (!empty($interval['start'])) {
                    $grouped[$globalId]['_all_dates'][] = [
                        'start' => $interval['start'],
                        'end' => $interval['end'] ?? null
                    ];
                }
            }
        }

        // Now process each grouped item to find earliest start and latest end
        foreach ($grouped as $globalId => &$item) {
            $allStarts = [];
            $allEnds = [];

            foreach ($item['_all_dates'] as $dateRange) {
                if (!empty($dateRange['start'])) {
                    $parsed = $this->parseDateTime($dateRange['start']);
                    if ($parsed) {
                        $allStarts[] = $parsed;
                    }
                }
                if (!empty($dateRange['end'])) {
                    $parsed = $this->parseDateTime($dateRange['end']);
                    if ($parsed) {
                        $allEnds[] = $parsed;
                    }
                }
            }

            // Store computed earliest/latest dates
            if (!empty($allStarts)) {
                sort($allStarts);
                $item['_computed_start'] = $allStarts[0];
            }
            if (!empty($allEnds)) {
                rsort($allEnds);
                $item['_computed_end'] = $allEnds[0];
            }

            // Clean up temporary data
            unset($item['_all_dates']);
        }

        return $grouped;
    }

    /**
     * Extract text by rel and type
     */
    protected function extractText(array $texts, string $rel, string $type): ?string
    {
        foreach ($texts as $text) {
            if (($text['rel'] ?? '') === $rel && ($text['type'] ?? '') === $type) {
                return $text['value'] ?? null;
            }
        }
        return null;
    }

    /**
     * Get attribute value by key
     */
    protected function getAttributeValue(array $attributes, string $key): ?string
    {
        foreach ($attributes as $attr) {
            if (($attr['key'] ?? '') === $key) {
                return $attr['value'] ?? null;
            }
        }
        return null;
    }

    /**
     * Strip HTML and clean text
     */
    protected function stripHtml(?string $html): ?string
    {
        if (empty($html)) {
            return null;
        }

        // Convert <br> and </p> to newlines
        $html = preg_replace('/<br\s*\/?>/i', "\n", $html);
        $html = preg_replace('/<\/p>/i', "\n\n", $html);

        // Strip tags
        $html = strip_tags($html);

        // Decode entities
        $html = html_entity_decode($html, ENT_QUOTES, 'UTF-8');

        // Clean whitespace
        $html = preg_replace('/\n{3,}/', "\n\n", $html);

        return trim($html);
    }

    /**
     * Clean unicode escaped HTML entities
     */
    protected function cleanUnicodeEscapes(?string $text): ?string
    {
        if (empty($text)) {
            return null;
        }

        // Replace common unicode escaped HTML tags
        $replacements = [
            '\u003C' => '<',
            '\u003E' => '>',
            '\u003c' => '<',
            '\u003e' => '>',
            '\u0026' => '&',
            '\u0022' => '"',
            '\u0027' => "'",
            '\\u003C' => '<',
            '\\u003E' => '>',
            '\\u003c' => '<',
            '\\u003e' => '>',
            '\\u0026' => '&',
            '\\u0022' => '"',
            '\\u0027' => "'",
        ];

        $text = str_replace(array_keys($replacements), array_values($replacements), $text);

        // Also decode any remaining JSON unicode escapes
        $text = preg_replace_callback('/\\\\u([0-9a-fA-F]{4})/', function ($matches) {
            return mb_convert_encoding(pack('H*', $matches[1]), 'UTF-8', 'UCS-2BE');
        }, $text);

        return $text;
    }

    /**
     * Clean and sanitize HTML while keeping formatting
     */
    protected function cleanHtml(?string $html): ?string
    {
        if (empty($html)) {
            return null;
        }

        // Decode HTML entities
        $html = html_entity_decode($html, ENT_QUOTES, 'UTF-8');

        // Allow only safe HTML tags
        $allowedTags = '<p><br><strong><b><em><i><u><ul><ol><li><a><h1><h2><h3><h4><h5><h6><span><div><blockquote>';
        $html = strip_tags($html, $allowedTags);

        // Clean up multiple consecutive line breaks
        $html = preg_replace('/(<br\s*\/?>\s*){3,}/i', '<br><br>', $html);

        // Clean up empty paragraphs
        $html = preg_replace('/<p>\s*<\/p>/i', '', $html);

        // Clean up excessive whitespace
        $html = preg_replace('/\s+/', ' ', $html);

        // But preserve line breaks
        $html = preg_replace('/<br\s*\/?>/i', '<br>', $html);
        $html = preg_replace('/<\/p>/i', '</p>', $html);

        return trim($html);
    }

    /**
     * Parse datetime string
     */
    protected function parseDateTime(?string $datetime): ?string
    {
        if (empty($datetime)) {
            return null;
        }

        try {
            return Carbon::parse($datetime)->format('Y-m-d H:i:s');
        } catch (\Exception $e) {
            return null;
        }
    }

    /**
     * Build place string
     */
    protected function buildPlace(array $item): ?string
    {
        $parts = [];

        if (!empty($item['name'])) {
            $parts[] = $item['name'];
        }
        if (!empty($item['street'])) {
            $parts[] = $item['street'];
        }

        $cityZip = [];
        if (!empty($item['zip'])) {
            $cityZip[] = $item['zip'];
        }
        if (!empty($item['city'])) {
            $cityZip[] = $item['city'];
        }
        if (!empty($cityZip)) {
            $parts[] = implode(' ', $cityZip);
        }

        return !empty($parts) ? implode(', ', $parts) : null;
    }

    /**
     * Generate unique slug
     */
    protected function generateUniqueSlug(string $title, string $identifier): string
    {
        $slug = str_slug($title);

        // Ensure slug is not too long
        $slug = mb_substr($slug, 0, 200);

        // Add hash for uniqueness
        $hash = substr(md5($identifier), 0, 8);

        return $slug . '-' . $hash;
    }

    /**
     * Map country name to country_id
     */
    protected function getCountryId(?string $country): ?int
    {
        if (empty($country)) {
            return null;
        }

        // Country name to ID mapping
        $countryMap = [
            'Germany' => 85,
            'Deutschland' => 85,
            'DE' => 85,
        ];

        // Check direct mapping (case-insensitive)
        foreach ($countryMap as $name => $id) {
            if (strcasecmp($country, $name) === 0) {
                return $id;
            }
        }

        // Try to find in database by name
        $dbCountry = Db::table('christophheich_calendar_countries')
            ->where('name', 'ILIKE', $country)
            ->first();

        if ($dbCountry) {
            return $dbCountry->id;
        }

        return null;
    }

    /**
     * Map API target group features and keywords to allowed target group values
     */
    protected function mapTargetGroups(array $features, array $keywords = []): array
    {
        // Allowed target groups in database
        $allowedGroups = [
            'Children (0-16 y)',
            'Young people (16-26 y)',
            'General public/Civil society',
            'Elderly people (+65y)',
            'Entrepreneurs/Businesses',
            'Policy makers',
            'Early career researchers',
            'All researchers',
        ];

        // Mapping from API values to database values
        $mapping = [
            // Children
            'target group children' => 'Children (0-16 y)',
            'target group kids' => 'Children (0-16 y)',
            'children' => 'Children (0-16 y)',
            'kids' => 'Children (0-16 y)',
            'child' => 'Children (0-16 y)',

            // Young people / Teenagers
            'target group teenager' => 'Young people (16-26 y)',
            'target group young adults' => 'Young people (16-26 y)',
            'target group youth' => 'Young people (16-26 y)',
            'teenager' => 'Young people (16-26 y)',
            'yadult' => 'Young people (16-26 y)',
            'talents' => 'Young people (16-26 y)',
            'young' => 'Young people (16-26 y)',
            'youth' => 'Young people (16-26 y)',
            'student' => 'Young people (16-26 y)',
            'students' => 'Young people (16-26 y)',

            // General public / Adults
            'target group adult' => 'General public/Civil society',
            'target group general public' => 'General public/Civil society',
            'target group civil society' => 'General public/Civil society',
            'adult' => 'General public/Civil society',
            'public' => 'General public/Civil society',
            'general' => 'General public/Civil society',
            'civil' => 'General public/Civil society',

            // Elderly
            'target group the elderly' => 'Elderly people (+65y)',
            'target group seniors' => 'Elderly people (+65y)',
            'senior' => 'Elderly people (+65y)',
            'seniors' => 'Elderly people (+65y)',
            'elderly' => 'Elderly people (+65y)',

            // Entrepreneurs/Businesses
            'target group entrepreneurs' => 'Entrepreneurs/Businesses',
            'target group businesses' => 'Entrepreneurs/Businesses',
            'target group business' => 'Entrepreneurs/Businesses',
            'entrepreneur' => 'Entrepreneurs/Businesses',
            'entrepreneurs' => 'Entrepreneurs/Businesses',
            'business' => 'Entrepreneurs/Businesses',
            'businesses' => 'Entrepreneurs/Businesses',
            'company' => 'Entrepreneurs/Businesses',
            'companies' => 'Entrepreneurs/Businesses',

            // Policy makers
            'target group policy makers' => 'Policy makers',
            'target group politicians' => 'Policy makers',
            'policy' => 'Policy makers',
            'policy maker' => 'Policy makers',
            'policy makers' => 'Policy makers',
            'politician' => 'Policy makers',
            'politicians' => 'Policy makers',
            'government' => 'Policy makers',

            // Early career researchers
            'target group early career researchers' => 'Early career researchers',
            'target group phd students' => 'Early career researchers',
            'target group postdocs' => 'Early career researchers',
            'phd' => 'Early career researchers',
            'postdoc' => 'Early career researchers',
            'postdocs' => 'Early career researchers',
            'early career' => 'Early career researchers',

            // All researchers
            'target group researchers' => 'All researchers',
            'target group scientists' => 'All researchers',
            'researcher' => 'All researchers',
            'researchers' => 'All researchers',
            'scientist' => 'All researchers',
            'scientists' => 'All researchers',
            'academic' => 'All researchers',
            'academics' => 'All researchers',

            // Teachers (map to general public)
            'teacher' => 'General public/Civil society',
            'teachers' => 'General public/Civil society',
            'target group teachers' => 'General public/Civil society',
        ];

        $mappedGroups = [];

        // Combine features and keywords for matching
        $allTerms = array_merge($features, $keywords);

        foreach ($allTerms as $term) {
            $termLower = strtolower(trim($term));

            // Check direct mapping
            if (isset($mapping[$termLower])) {
                $mappedGroups[] = $mapping[$termLower];
                continue;
            }

            // Check if term contains any mapping key
            foreach ($mapping as $key => $value) {
                if (strpos($termLower, $key) !== false) {
                    $mappedGroups[] = $value;
                    break;
                }
            }
        }

        // Remove duplicates and return
        return array_unique($mappedGroups);
    }

    /**
     * Map API categories and keywords to allowed thematic focuses
     */
    protected function mapThematicFocuses(array $categories, array $keywords = []): array
    {
        // Allowed thematic focuses
        $allowedFocuses = [
            'Sea and water',
            'Europe',
            'Sustainability',
            'Other',
            'Cultural Heritage',
            'Health and well-being',
            'Talents',
        ];

        // Mapping from API values to thematic focuses
        $mapping = [
            // Sea and water
            'sea' => 'Sea and water',
            'water' => 'Sea and water',
            'ocean' => 'Sea and water',
            'marine' => 'Sea and water',
            'maritime' => 'Sea and water',
            'coastal' => 'Sea and water',
            'fisheries' => 'Sea and water',
            'aquatic' => 'Sea and water',

            // Europe
            'europe' => 'Europe',
            'european' => 'Europe',
            'eu' => 'Europe',

            // Sustainability
            'sustainability' => 'Sustainability',
            'sustainable' => 'Sustainability',
            'environment' => 'Sustainability',
            'environmental' => 'Sustainability',
            'climate' => 'Sustainability',
            'green' => 'Sustainability',
            'ecology' => 'Sustainability',
            'ecological' => 'Sustainability',
            'renewable' => 'Sustainability',
            'energy' => 'Sustainability',

            // Cultural Heritage
            'cultural heritage' => 'Cultural Heritage',
            'culture' => 'Cultural Heritage',
            'heritage' => 'Cultural Heritage',
            'history' => 'Cultural Heritage',
            'historical' => 'Cultural Heritage',
            'museum' => 'Cultural Heritage',
            'archaeology' => 'Cultural Heritage',
            'art' => 'Cultural Heritage',
            'arts' => 'Cultural Heritage',

            // Health and well-being
            'health' => 'Health and well-being',
            'well-being' => 'Health and well-being',
            'wellbeing' => 'Health and well-being',
            'medicine' => 'Health and well-being',
            'medical' => 'Health and well-being',
            'wellness' => 'Health and well-being',
            'mental health' => 'Health and well-being',
            'healthcare' => 'Health and well-being',

            // Talents
            'talents' => 'Talents',
            'talent' => 'Talents',
            'education' => 'Talents',
            'students' => 'Talents',
            'youth' => 'Talents',
            'skills' => 'Talents',
            'training' => 'Talents',
            'career' => 'Talents',
        ];

        $mappedFocuses = [];

        // Combine categories and keywords for matching
        $allTerms = array_merge($categories, $keywords);

        foreach ($allTerms as $term) {
            $termLower = strtolower(trim($term));

            // Check direct mapping
            if (isset($mapping[$termLower])) {
                $mappedFocuses[] = $mapping[$termLower];
                continue;
            }

            // Check if term contains any mapping key
            foreach ($mapping as $key => $value) {
                if (strpos($termLower, $key) !== false) {
                    $mappedFocuses[] = $value;
                    break;
                }
            }
        }

        // Remove duplicates
        $mappedFocuses = array_unique($mappedFocuses);

        // If no matches found, return 'Other'
        if (empty($mappedFocuses)) {
            return ['Other'];
        }

        return $mappedFocuses;
    }

    /**
     * Attach default category to entry if not already attached
     */
    protected function attachDefaultCategory(int $entryId, int $categoryId = 1): bool
    {
        $pivotTable = 'pensoft_calendar_entries_categories';

        // Check if category is already attached
        $exists = Db::table($pivotTable)
            ->where('entry_id', $entryId)
            ->where('category_id', $categoryId)
            ->exists();

        if ($exists) {
            return false; // Already attached
        }

        // Attach default category
        try {
            Db::table($pivotTable)->insert([
                'entry_id' => $entryId,
                'category_id' => $categoryId,
            ]);
            return true;
        } catch (\Exception $e) {
            Log::warning('Events Import: Failed to attach category', [
                'entry_id' => $entryId,
                'category_id' => $categoryId,
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }

    /**
     * Upload cover image from API media objects
     */
    protected function uploadCoverImage(int $entryId, array $item, bool $isNew = true): bool
    {
        // Check if cover image already exists (for updates)
        if (!$isNew) {
            $existingImage = Db::table('system_files')
                ->where('attachment_type', 'Pensoft\Calendar\Models\Entry')
                ->where('attachment_id', $entryId)
                ->where('field', 'cover_image')
                ->first();

            if ($existingImage) {
                return false; // Already has cover image
            }
        }

        // Find image URL from media_objects
        $imageUrl = null;
        $imageName = null;

        foreach ($item['media_objects'] ?? [] as $media) {
            // Look for image types
            if (in_array($media['rel'] ?? '', ['image', 'teaser', 'default', 'primary', 'main'])) {
                if (!empty($media['url'])) {
                    $imageUrl = $media['url'];
                    $imageName = $media['name'] ?? basename(parse_url($imageUrl, PHP_URL_PATH));
                    break;
                }
            }
        }

        // Fallback: try to find any image URL
        if (!$imageUrl) {
            foreach ($item['media_objects'] ?? [] as $media) {
                $url = $media['url'] ?? '';
                if (preg_match('/\.(jpg|jpeg|png|gif|webp)$/i', $url)) {
                    $imageUrl = $url;
                    $imageName = $media['name'] ?? basename(parse_url($imageUrl, PHP_URL_PATH));
                    break;
                }
            }
        }

        if (!$imageUrl) {
            return false; // No image found
        }

        try {
            // Download image to temp file
            $context = stream_context_create([
                'http' => [
                    'timeout' => 30,
                    'header' => "Accept: image/*\r\n"
                ]
            ]);

            $imageData = file_get_contents($imageUrl, false, $context);

            if ($imageData === false) {
                Log::warning('Events Import: Failed to download image', ['url' => $imageUrl]);
                return false;
            }

            // Create temp file
            $tempPath = temp_path('import_' . uniqid() . '_' . $imageName);
            file_put_contents($tempPath, $imageData);

            // Create File model and attach
            $file = new File();
            $file->fromFile($tempPath);
            $file->attachment_type = 'Pensoft\Calendar\Models\Entry';
            $file->attachment_id = $entryId;
            $file->field = 'cover_image';
            $file->is_public = true;
            $file->save();

            // Clean up temp file
            if (file_exists($tempPath)) {
                unlink($tempPath);
            }

            return true;

        } catch (\Exception $e) {
            Log::error('Events Import: Failed to upload cover image', [
                'entry_id' => $entryId,
                'url' => $imageUrl,
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }

    /**
     * Handle update matching mode - update entries where title, start and end dates match
     * Updates only non-key fields (keeps title, start, end, slug unchanged)
     */
    protected function handleUpdateMatching(string $url, bool $dryRun): int
    {
        $this->info("Starting update matching entries...");
        $this->info("API URL: {$url}");

        // Fetch JSON from API
        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 60,
                    'header' => "Accept: application/json\r\n"
                ]
            ]);

            $response = file_get_contents($url, false, $context);

            if ($response === false) {
                $this->error('Failed to fetch data from API');
                return 1;
            }
        } catch (\Exception $e) {
            $this->error('API request failed: ' . $e->getMessage());
            return 1;
        }

        $data = json_decode($response, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error('Invalid JSON response: ' . json_last_error_msg());
            return 1;
        }

        if (!isset($data['items']) || !is_array($data['items'])) {
            $this->error('JSON does not contain items array');
            return 1;
        }

        // Pre-process: Group items by global_id and collect all dates
        $groupedItems = $this->groupItemsByGlobalId($data['items']);
        $this->info("Found " . count($groupedItems) . " unique events in API");

        $stats = [
            'updated' => 0,
            'not_found' => 0,
            'skipped' => 0,
            'errors' => 0,
        ];

        $progressBar = $this->output->createProgressBar(count($groupedItems));
        $progressBar->start();

        foreach ($groupedItems as $globalId => $item) {
            $progressBar->advance();

            try {
                // Skip items without required fields
                if (empty($item['title'])) {
                    $stats['skipped']++;
                    continue;
                }

                // Transform to get the data we need
                $entry = $this->transformItem($item, $globalId);

                // Find matching entry by title and dates (ignoring time)
                $matchingEntry = $this->findMatchingEntry($entry['title'], $entry['start'], $entry['end']);

                if (!$matchingEntry) {
                    $stats['not_found']++;
                    continue;
                }

                if ($dryRun) {
                    $stats['updated']++;
                    continue;
                }

                // Prepare update data (all fields except title, start, end, slug)
                $updateData = [
                    'description' => $entry['description'],
                    'url' => $entry['url'],
                    'place' => $entry['place'],
                    'country_id' => $entry['country_id'],
                    'institution' => $entry['institution'],
                    'contact' => $entry['contact'],
                    'email' => $entry['email'],
                    'theme' => $entry['theme'],
                    'format' => $entry['format'],
                    'target' => $entry['target'],
                    'tags' => $entry['tags'],
                    'fee' => $entry['fee'],
                    'meta_title' => $entry['meta_title'],
                    'meta_description' => $entry['meta_description'],
                    'meta_keywords' => $entry['meta_keywords'],
                    'source' => $entry['source'],
                    'identifier' => $globalId,
                    'updated_at' => Carbon::now(),
                    'deleted_at' => null,
                ];

                // Update the entry
                Db::table($this->table)
                    ->where('id', $matchingEntry->id)
                    ->update($updateData);

                // Upload cover image if not already set
                $this->uploadCoverImage($matchingEntry->id, $item, false);

                // Attach default category if not already attached
                $this->attachDefaultCategory($matchingEntry->id);

                $stats['updated']++;

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing item: ' . ($item['global_id'] ?? 'unknown'));
                $this->error('Message: ' . $e->getMessage());
                Log::error('Events Import: Error in update matching', [
                    'global_id' => $item['global_id'] ?? 'unknown',
                    'error' => $e->getMessage()
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

        // Summary
        $this->info("Update matching completed!" . ($dryRun ? ' (DRY RUN)' : ''));
        $this->table(
            ['Metric', 'Count'],
            [
                ['Updated', $stats['updated']],
                ['Not found', $stats['not_found']],
                ['Skipped', $stats['skipped']],
                ['Errors', $stats['errors']],
            ]
        );

        Log::info('Events Import: Update matching completed', $stats);

        return 0;
    }

    /**
     * Handle update ALL fields for matching entries (by title and dates)
     * Updates ALL fields including title, start, end, slug, and replaces cover image
     */
    protected function handleUpdateAllMatching(string $url, bool $dryRun): int
    {
        $this->info("Starting update ALL fields for matching entries...");
        $this->info("API URL: {$url}");

        // Fetch JSON from API
        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 60,
                    'header' => "Accept: application/json\r\n"
                ]
            ]);

            $response = file_get_contents($url, false, $context);

            if ($response === false) {
                $this->error('Failed to fetch data from API');
                return 1;
            }
        } catch (\Exception $e) {
            $this->error('API request failed: ' . $e->getMessage());
            return 1;
        }

        $data = json_decode($response, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error('Invalid JSON response: ' . json_last_error_msg());
            return 1;
        }

        if (!isset($data['items']) || !is_array($data['items'])) {
            $this->error('JSON does not contain items array');
            return 1;
        }

        // Pre-process: Group items by global_id and collect all dates
        $groupedItems = $this->groupItemsByGlobalId($data['items']);
        $this->info("Found " . count($groupedItems) . " unique events in API");

        $stats = [
            'updated' => 0,
            'not_found' => 0,
            'skipped' => 0,
            'errors' => 0,
        ];

        $progressBar = $this->output->createProgressBar(count($groupedItems));
        $progressBar->start();

        foreach ($groupedItems as $globalId => $item) {
            $progressBar->advance();

            try {
                // Skip items without required fields
                if (empty($item['title'])) {
                    $stats['skipped']++;
                    continue;
                }

                // Transform to get the data we need
                $entry = $this->transformItem($item, $globalId);

                // Find matching entry by title and dates (ignoring time)
                $matchingEntry = $this->findMatchingEntry($entry['title'], $entry['start'], $entry['end']);

                if (!$matchingEntry) {
                    $stats['not_found']++;
                    continue;
                }

                if ($dryRun) {
                    $stats['updated']++;
                    continue;
                }

                // Prepare update data - ALL fields including title, start, end, slug
                $updateData = [
                    'title' => $entry['title'],
                    'slug' => $entry['slug'],
                    'identifier' => $globalId,
                    'start' => $entry['start'],
                    'end' => $entry['end'],
                    'all_day' => $entry['all_day'],
                    'description' => $entry['description'],
                    'url' => $entry['url'],
                    'place' => $entry['place'],
                    'country_id' => $entry['country_id'],
                    'institution' => $entry['institution'],
                    'contact' => $entry['contact'],
                    'email' => $entry['email'],
                    'theme' => $entry['theme'],
                    'format' => $entry['format'],
                    'target' => $entry['target'],
                    'tags' => $entry['tags'],
                    'fee' => $entry['fee'],
                    'meta_title' => $entry['meta_title'],
                    'meta_description' => $entry['meta_description'],
                    'meta_keywords' => $entry['meta_keywords'],
                    'is_public' => $entry['is_public'],
                    'is_internal' => $entry['is_internal'],
                    'show_on_timeline' => $entry['show_on_timeline'],
                    'source' => $entry['source'],
                    'updated_at' => Carbon::now(),
                    'deleted_at' => null,
                ];

                // Update the entry
                Db::table($this->table)
                    ->where('id', $matchingEntry->id)
                    ->update($updateData);

                // Delete existing cover image and upload new one
                Db::table('system_files')
                    ->where('attachment_type', 'Pensoft\Calendar\Models\Entry')
                    ->where('attachment_id', $matchingEntry->id)
                    ->where('field', 'cover_image')
                    ->delete();

                $this->uploadCoverImage($matchingEntry->id, $item, true);

                // Re-attach default category
                $this->attachDefaultCategory($matchingEntry->id);

                $stats['updated']++;

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing item: ' . ($item['global_id'] ?? 'unknown'));
                $this->error('Message: ' . $e->getMessage());
                Log::error('Events Import: Error in update all matching', [
                    'global_id' => $item['global_id'] ?? 'unknown',
                    'error' => $e->getMessage()
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

        // Summary
        $this->info("Update ALL fields completed!" . ($dryRun ? ' (DRY RUN)' : ''));
        $this->table(
            ['Metric', 'Count'],
            [
                ['Updated', $stats['updated']],
                ['Not found', $stats['not_found']],
                ['Skipped', $stats['skipped']],
                ['Errors', $stats['errors']],
            ]
        );

        Log::info('Events Import: Update all matching completed', $stats);

        return 0;
    }

    /**
     * Find entry matching by title, start date and end date (ignoring time)
     */
    protected function findMatchingEntry(string $title, ?string $start, ?string $end)
    {
        $query = Db::table($this->table)
            ->where('title', $title);

        // Compare start date (date only, ignore time)
        if ($start) {
            $startDate = Carbon::parse($start)->format('Y-m-d');
            $query->whereRaw('DATE("start") = ?', [$startDate]);
        } else {
            $query->whereNull('start');
        }

        // Compare end date (date only, ignore time)
        if ($end) {
            $endDate = Carbon::parse($end)->format('Y-m-d');
            $query->whereRaw('DATE("end") = ?', [$endDate]);
        } else {
            $query->whereNull('end');
        }

        return $query->first();
    }

    /**
     * Handle populate missing data mode
     */
    protected function handlePopulateMissing(string $url, bool $dryRun): int
    {
        $this->info("Starting populate missing data...");
        $this->info("API URL: {$url}");

        // Fetch JSON from API
        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 60,
                    'header' => "Accept: application/json\r\n"
                ]
            ]);

            $response = file_get_contents($url, false, $context);

            if ($response === false) {
                $this->error('Failed to fetch data from API');
                return 1;
            }
        } catch (\Exception $e) {
            $this->error('API request failed: ' . $e->getMessage());
            return 1;
        }

        $data = json_decode($response, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error('Invalid JSON response: ' . json_last_error_msg());
            return 1;
        }

        if (!isset($data['items']) || !is_array($data['items'])) {
            $this->error('JSON does not contain items array');
            return 1;
        }

        // Build lookup array from API items by global_id
        $apiItems = [];
        foreach ($data['items'] as $item) {
            if (!empty($item['global_id'])) {
                $apiItems[$item['global_id']] = $item;
            }
        }

        $this->info("Found " . count($apiItems) . " unique items in API");

        // Get all existing imported entries
        $existingEntries = Db::table($this->table)
            ->where('is_internal', false)
            ->whereNotNull('identifier')
            ->get();

        $this->info("Found " . count($existingEntries) . " existing imported entries");

        $stats = [
            'cover_images_added' => 0,
            'categories_added' => 0,
            'countries_updated' => 0,
            'descriptions_updated' => 0,
            'places_updated' => 0,
            'urls_updated' => 0,
            'themes_updated' => 0,
            'targets_updated' => 0,
            'skipped' => 0,
            'not_found' => 0,
            'errors' => 0,
        ];

        $progressBar = $this->output->createProgressBar(count($existingEntries));
        $progressBar->start();

        foreach ($existingEntries as $entry) {
            $progressBar->advance();

            try {
                // Extract global_id from identifier (remove any suffix)
                $globalId = $entry->identifier;
                if (strpos($globalId, '_') !== false) {
                    $globalId = explode('_', $globalId)[0];
                }

                // Find matching API item
                if (!isset($apiItems[$globalId])) {
                    $stats['not_found']++;
                    continue;
                }

                $item = $apiItems[$globalId];
                $updates = [];

                // Check and populate missing cover image
                $hasCoverImage = Db::table('system_files')
                    ->where('attachment_type', 'Pensoft\Calendar\Models\Entry')
                    ->where('attachment_id', $entry->id)
                    ->where('field', 'cover_image')
                    ->exists();

                if (!$hasCoverImage && !$dryRun) {
                    if ($this->uploadCoverImage($entry->id, $item, true)) {
                        $stats['cover_images_added']++;
                    }
                } elseif (!$hasCoverImage && $dryRun) {
                    $stats['cover_images_added']++;
                }

                // Check and attach default category if not already attached
                $hasCategory = Db::table('pensoft_calendar_entries_categories')
                    ->where('entry_id', $entry->id)
                    ->exists();

                if (!$hasCategory && !$dryRun) {
                    if ($this->attachDefaultCategory($entry->id)) {
                        $stats['categories_added']++;
                    }
                } elseif (!$hasCategory && $dryRun) {
                    $stats['categories_added']++;
                }

                // Check and populate missing description
                if (empty($entry->description)) {
                    $description = $this->extractText($item['texts'] ?? [], 'details', 'text/html');
                    if (empty($description)) {
                        $description = $this->extractText($item['texts'] ?? [], 'details', 'text/plain');
                        if ($description) {
                            $description = nl2br($description);
                        }
                    }
                    if ($description) {
                        $description = $this->cleanUnicodeEscapes($description);
                        $description = $this->cleanHtml($description);
                    }
                    if (!empty($description)) {
                        $updates['description'] = $description;
                        $stats['descriptions_updated']++;
                    }
                }

                // Check and populate missing place
                if (empty($entry->place)) {
                    $place = $this->buildPlace($item);
                    if (!empty($place)) {
                        $updates['place'] = mb_substr($place, 0, 255);
                        $stats['places_updated']++;
                    }
                }

                // Check and populate missing URL
                if (empty($entry->url)) {
                    $entryUrl = $item['web'] ?? null;
                    foreach ($item['media_objects'] ?? [] as $media) {
                        if ($media['rel'] === 'venuewebsite' && !empty($media['url'])) {
                            $entryUrl = $media['url'];
                            break;
                        }
                    }
                    if (!empty($entryUrl)) {
                        $updates['url'] = mb_substr($entryUrl, 0, 255);
                        $stats['urls_updated']++;
                    }
                }

                // Check and populate missing country_id
                if (empty($entry->country_id)) {
                    $countryId = $this->getCountryId($item['country'] ?? null);
                    if (!empty($countryId)) {
                        $updates['country_id'] = $countryId;
                        $stats['countries_updated']++;
                    }
                }

                // Check and populate missing theme
                if (empty($entry->theme)) {
                    $categories = $item['categories'] ?? [];
                    $keywords = $item['keywords'] ?? [];
                    $mappedThemes = $this->mapThematicFocuses($categories, $keywords);
                    if (!empty($mappedThemes)) {
                        $updates['theme'] = mb_substr(implode(', ', $mappedThemes), 0, 255);
                        $stats['themes_updated']++;
                    }
                }

                // Check and populate missing target
                if (empty($entry->target)) {
                    $features = $item['features'] ?? [];
                    $keywords = $item['keywords'] ?? [];
                    $targetGroups = $this->mapTargetGroups($features, $keywords);
                    if (!empty($targetGroups)) {
                        $updates['target'] = mb_substr(implode(', ', $targetGroups), 0, 255);
                        $stats['targets_updated']++;
                    }
                }

                // Apply updates if any
                if (!empty($updates) && !$dryRun) {
                    $updates['updated_at'] = Carbon::now();
                    Db::table($this->table)
                        ->where('id', $entry->id)
                        ->update($updates);
                }

                if (empty($updates) && !$hasCoverImage === false) {
                    $stats['skipped']++;
                }

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing entry ID: ' . $entry->id);
                $this->error('Message: ' . $e->getMessage());
                Log::error('Events Import: Error populating missing data', [
                    'entry_id' => $entry->id,
                    'error' => $e->getMessage()
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

        // Summary
        $this->info("Populate missing data completed!" . ($dryRun ? ' (DRY RUN)' : ''));
        $this->table(
            ['Metric', 'Count'],
            [
                ['Cover images added', $stats['cover_images_added']],
                ['Categories added', $stats['categories_added']],
                ['Countries updated', $stats['countries_updated']],
                ['Descriptions updated', $stats['descriptions_updated']],
                ['Places updated', $stats['places_updated']],
                ['URLs updated', $stats['urls_updated']],
                ['Themes updated', $stats['themes_updated']],
                ['Targets updated', $stats['targets_updated']],
                ['Skipped (complete)', $stats['skipped']],
                ['Not found in API', $stats['not_found']],
                ['Errors', $stats['errors']],
            ]
        );

        Log::info('Events Import: Populate missing completed', $stats);

        return 0;
    }
}
