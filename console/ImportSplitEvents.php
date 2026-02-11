<?php

namespace Pensoft\Eventsfields\Console;

use Illuminate\Console\Command;
use Db;
use Carbon\Carbon;
use Log;
use System\Models\File;

class ImportSplitEvents extends Command
{
    /**
     * @var string The console command name.
     */
    protected $name = 'events:import-split';

    /**
     * @var string The console command description.
     */
    protected $description = 'Import events from Split.hr API';

    /**
     * @var string The console command signature.
     */
    protected $signature = 'events:import-split
                            {--url= : API URL to fetch JSON from (overrides config)}
                            {--dry-run : Preview import without saving}
                            {--populate-missing : Populate missing data for existing entries}
                            {--update-matching : Update entries matching by global_id (articleId)}
                            {--update-all-matching : Update ALL fields for entries matching by global_id (articleId)}';

    /**
     * @var string Table name
     */
    protected $table = 'christophheich_calendar_entries';

    /**
     * @var string Source identifier for entries imported via this command
     */
    protected $sourceName = 'split.hr';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        // Get URL from option, settings, or fail
        $url = $this->option('url');

        if (!$url) {
            $settings = \Pensoft\Eventsfields\Models\Settings::instance();
            $url = $settings->split_api_url ?? null;
        }

        if (empty($url)) {
            $this->error('No Split API URL configured. Set it in Settings or use --url option.');
            return 1;
        }

        // Check if import is enabled (skip check if running manually with --url)
        if (!$this->option('url')) {
            $settings = \Pensoft\Eventsfields\Models\Settings::instance();
            if (!$settings->split_import_enabled) {
                $this->warn('Split.hr automatic import is disabled in settings.');
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

        $this->info("Starting Split.hr events import...");
        $this->info("API URL: {$url}");

        // Fetch and parse API data
        $groupedItems = $this->fetchAndGroupItems($url);
        if ($groupedItems === null) {
            return 1;
        }

        $this->info("Found " . count($groupedItems) . " unique events after grouping");

        $stats = [
            'inserted' => 0,
            'updated' => 0,
            'skipped' => 0,
            'errors' => 0,
        ];

        $processedIdentifiers = [];

        $progressBar = $this->output->createProgressBar(count($groupedItems));
        $progressBar->start();

        foreach ($groupedItems as $globalId => $item) {
            $progressBar->advance();

            try {
                // Skip items without required fields
                if (empty($item['articleId']) || empty($item['title'])) {
                    $stats['skipped']++;
                    continue;
                }

                $uniqueId = $this->buildGlobalId($item['articleId']);

                // Skip if already processed in this import
                if (in_array($uniqueId, $processedIdentifiers)) {
                    $stats['skipped']++;
                    continue;
                }

                $processedIdentifiers[] = $uniqueId;

                // Transform item to entry
                $entry = $this->transformItem($item, $uniqueId);

                // Check if exists by global_id (for updates) - include soft-deleted
                $existing = $this->findMatchingEntryByGlobalId($uniqueId);

                if ($existing) {
                    // Entry already exists – update it
                    if ($dryRun) {
                        $stats['updated']++;
                        continue;
                    }

                    $entry['updated_at'] = Carbon::now();
                    $entry['deleted_at'] = null;
                    unset($entry['created_at']);

                    Db::table($this->table)
                        ->where('id', $existing->id)
                        ->update($entry);
                    $stats['updated']++;

                    // Upload cover image if available and not already set
                    $this->uploadCoverImage($existing->id, $item, false);

                    // Attach default category if not already attached
                    $this->attachDefaultCategory($existing->id);
                } else {
                    // New entry – insert
                    if ($dryRun) {
                        $stats['inserted']++;
                        continue;
                    }

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
                $this->error('Error processing item: ' . ($item['articleId'] ?? 'unknown'));
                $this->error('Message: ' . $e->getMessage());
                $this->error('File: ' . $e->getFile() . ':' . $e->getLine());
                Log::error('Split Events Import: Error processing item', [
                    'articleId' => $item['articleId'] ?? 'unknown',
                    'error' => $e->getMessage(),
                    'file' => $e->getFile(),
                    'line' => $e->getLine(),
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

        // Summary
        $this->info("Import completed!" . ($dryRun ? ' (DRY RUN)' : ''));
        $this->table(
            ['Metric', 'Count'],
            [
                ['Inserted', $stats['inserted']],
                ['Updated', $stats['updated']],
                ['Skipped', $stats['skipped']],
                ['Errors', $stats['errors']],
                ['Unique events', count($processedIdentifiers)],
            ]
        );

        Log::info('Split Events Import completed', $stats);

        return 0;
    }

    // =========================================================================
    //  API FETCHING & GROUPING
    // =========================================================================

    /**
     * Fetch JSON from the Split API and return grouped items.
     * Returns null on failure.
     */
    protected function fetchAndGroupItems(string $url): ?array
    {
        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 60,
                    'header' => "Accept: application/json\r\n",
                ],
            ]);

            $response = file_get_contents($url, false, $context);

            if ($response === false) {
                $this->error('Failed to fetch data from API');
                Log::error('Split Events Import: Failed to fetch data from API', ['url' => $url]);
                return null;
            }
        } catch (\Exception $e) {
            $this->error('API request failed: ' . $e->getMessage());
            Log::error('Split Events Import: API request failed', ['error' => $e->getMessage()]);
            return null;
        }

        $data = json_decode($response, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error('Invalid JSON response: ' . json_last_error_msg());
            Log::error('Split Events Import: Invalid JSON', ['error' => json_last_error_msg()]);
            return null;
        }

        // The Split API returns a root array (not wrapped in {items: [...]})
        if (!is_array($data)) {
            $this->error('JSON response is not an array');
            return null;
        }

        $totalItems = count($data);
        $this->info("Found {$totalItems} items in API response");

        return $this->groupItemsByArticleId($data);
    }

    /**
     * Group items by articleId and collect all event date ranges from recurring entries.
     * Multiple entries with the same articleId but different recurringId represent
     * different date spans of the same event.
     */
    protected function groupItemsByArticleId(array $items): array
    {
        $grouped = [];

        foreach ($items as $item) {
            $articleId = $item['articleId'] ?? null;

            if (empty($articleId)) {
                continue;
            }

            if (!isset($grouped[$articleId])) {
                // First occurrence – use as the base item
                $grouped[$articleId] = $item;
                $grouped[$articleId]['_all_dates'] = [];
            }

            // Collect date range from eventInfo
            $eventInfo = $item['eventInfo'] ?? null;
            if ($eventInfo && !empty($eventInfo['startDateUTC'])) {
                $grouped[$articleId]['_all_dates'][] = [
                    'start' => $eventInfo['startDateUTC'],
                    'end'   => $eventInfo['endDateUTC'] ?? null,
                ];
            }
        }

        // Compute earliest start / latest end for each group
        foreach ($grouped as $articleId => &$item) {
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

            if (!empty($allStarts)) {
                sort($allStarts);
                $item['_computed_start'] = $allStarts[0];
            }
            if (!empty($allEnds)) {
                rsort($allEnds);
                $item['_computed_end'] = $allEnds[0];
            }

            unset($item['_all_dates']);
        }

        return $grouped;
    }

    // =========================================================================
    //  TRANSFORM
    // =========================================================================

    /**
     * Transform a Split API item into a database entry array.
     */
    protected function transformItem(array $item, string $uniqueId): array
    {
        // ── Description ─────────────────────────────────────────────────
        $description = $this->decodeHtmlField($item['articleText'] ?? null);
        if (empty($description)) {
            $description = $this->decodeHtmlField($item['summary'] ?? null);
        }
        if ($description) {
            $description = $this->cleanHtml($description);
        }

        // ── Teaser / meta description ───────────────────────────────────
        $teaser = $this->decodeHtmlField($item['summary'] ?? null);
        if ($teaser) {
            $teaser = $this->stripHtml($teaser);
        }

        // ── Dates ───────────────────────────────────────────────────────
        $start = $item['_computed_start'] ?? null;
        $end   = $item['_computed_end'] ?? null;

        // Fallback: single eventInfo
        if (!$start && !empty($item['eventInfo']['startDateUTC'])) {
            $start = $this->parseDateTime($item['eventInfo']['startDateUTC']);
            $end   = $this->parseDateTime($item['eventInfo']['endDateUTC'] ?? null);
        }

        // All day flag
        $allDay = false;
        if (!empty($item['eventInfo'])) {
            $allDay = !empty($item['eventInfo']['wholeDay']);
        }
        // Also detect midnight-to-midnight pattern
        if (!$allDay && $start && $end) {
            $startTime = Carbon::parse($start)->format('H:i:s');
            $endTime   = Carbon::parse($end)->format('H:i:s');
            if ($startTime === '00:00:00' && $endTime === '23:59:59') {
                $allDay = true;
            }
        }

        // ── URL ─────────────────────────────────────────────────────────
        $url = $item['articlelUrl'] ?? null;
        // Check articleLinkList for a more specific URL
        foreach ($item['articleLinkList'] ?? [] as $link) {
            if (!empty($link['URL'])) {
                $url = $link['URL'];
                break;
            }
        }

        // ── Author / contact ──────────────────────────────────────────
        $author = $item['author'] ?? null;

        // ── Extract customFieldList values ──────────────────────────────
        $customFields = $this->extractCustomFields($item['customFieldList'] ?? []);

        // ── Categories ──────────────────────────────────────────────────
        $categoryNames = [];
        foreach ($item['articleCategories'] ?? [] as $cat) {
            if (!empty($cat['name'])) {
                $categoryNames[] = $cat['name'];
            }
        }

        // ── Themes: prefer customFieldList, fall back to mapping ────────
        if (!empty($customFields['Theme'])) {
            $theme = $customFields['Theme'];
        } else {
            $mappedThemes = $this->mapThematicFocuses($categoryNames, []);
            $theme = !empty($mappedThemes) ? implode(', ', $mappedThemes) : null;
        }

        // ── Tags ────────────────────────────────────────────────────────
        $tags = !empty($categoryNames) ? implode(', ', $categoryNames) : null;

        // ── Target groups: prefer customFieldList, fall back to mapping ─
        if (!empty($customFields['Target groups'])) {
            $target = $customFields['Target groups'];
        } else {
            $targetTerms = array_merge($categoryNames, $this->extractTargetHints($description));
            $targetGroups = $this->mapTargetGroups($targetTerms, []);
            $target = !empty($targetGroups) ? implode(', ', $targetGroups) : null;
        }

        // ── Extract contact info from articleText HTML ──────────────────
        $decodedText = $this->decodeHtmlField($item['articleText'] ?? null);
        $parsedContact = $this->parseContactFromHtml($decodedText);

        // Use parsed email if available, otherwise null
        $email = $parsedContact['email'];

        // Use parsed contact person if available, fall back to author
        $contact = $parsedContact['contact'] ?? $author;
        if (empty($contact)) {
            $contact = $author;
        }

        // Use parsed place if available
        $place = $parsedContact['place'];

        // Use parsed institution if available
        $institution = $parsedContact['institution'];

        // ── Slug ────────────────────────────────────────────────────────
        $slug = $this->generateUniqueSlug($item['title'], $uniqueId);

        // ── Country ─────────────────────────────────────────────────────
        $countryId = $this->getCountryId('Croatia');

        return [
            'title'            => mb_substr($item['title'] ?? '', 0, 255),
            'slug'             => $slug,
            'global_id'        => $uniqueId,
            'identifier'       => $uniqueId,
            'start'            => $start,
            'end'              => $end,
            'all_day'          => $allDay,
            'description'      => $description,
            'url'              => $url ? mb_substr($url, 0, 255) : null,
            'place'            => $place ? mb_substr($place, 0, 255) : null,
            'country_id'       => $countryId,
            'institution'      => $institution ? mb_substr($institution, 0, 255) : null,
            'contact'          => $contact ? mb_substr($contact, 0, 255) : null,
            'email'            => $email ? mb_substr($email, 0, 255) : null,
            'theme'            => $theme ? mb_substr($theme, 0, 255) : null,
            'format'           => $parsedContact['format'] ? mb_substr($parsedContact['format'], 0, 255) : null,
            'target'           => $target ? mb_substr($target, 0, 255) : null,
            'tags'             => $tags ? mb_substr($tags, 0, 255) : null,
            'fee'              => null,
            'meta_title'       => mb_substr($item['title'] ?? '', 0, 255),
            'meta_description' => $teaser ? mb_substr($teaser, 0, 255) : null,
            'meta_keywords'    => $tags ? mb_substr($tags, 0, 255) : null,
            'is_public'        => true,
            'is_internal'      => false,
            'show_on_timeline' => false,
            'source'           => $this->sourceName,
        ];
    }

    // =========================================================================
    //  DUPLICATE / LOOKUP HELPERS
    // =========================================================================

    /**
     * Build a namespaced global_id from an articleId to avoid collisions
     * with the destination.one importer.
     */
    protected function buildGlobalId($articleId): string
    {
        return 'split-' . $articleId;
    }

    /**
     * Find entry matching by global_id, with fallback to identifier
     */
    protected function findMatchingEntryByGlobalId(string $globalId)
    {
        $entry = Db::table($this->table)
            ->where('global_id', $globalId)
            ->first();

        if ($entry) {
            return $entry;
        }

        return Db::table($this->table)
            ->where('identifier', $globalId)
            ->first();
    }

    // =========================================================================
    //  TEXT / HTML HELPERS
    // =========================================================================

    /**
     * Decode an HTML-entity-encoded field from the Split API.
     * The API returns HTML with entities like &lt;p&gt; instead of <p>.
     */
    protected function decodeHtmlField(?string $value): ?string
    {
        if (empty($value)) {
            return null;
        }

        // Decode HTML entities (may be double-encoded)
        $value = html_entity_decode($value, ENT_QUOTES, 'UTF-8');
        $value = html_entity_decode($value, ENT_QUOTES, 'UTF-8');

        return trim($value) ?: null;
    }

    /**
     * Strip HTML tags and clean text for plain-text fields
     */
    protected function stripHtml(?string $html): ?string
    {
        if (empty($html)) {
            return null;
        }

        $html = preg_replace('/<br\s*\/?>/i', "\n", $html);
        $html = preg_replace('/<\/p>/i', "\n\n", $html);
        $html = strip_tags($html);
        $html = html_entity_decode($html, ENT_QUOTES, 'UTF-8');
        $html = preg_replace('/\n{3,}/', "\n\n", $html);
        // Collapse &nbsp; sequences
        $html = preg_replace('/(\x{00A0}|\s)+/u', ' ', $html);

        return trim($html) ?: null;
    }

    /**
     * Clean and sanitize HTML while keeping safe formatting tags
     */
    protected function cleanHtml(?string $html): ?string
    {
        if (empty($html)) {
            return null;
        }

        $html = html_entity_decode($html, ENT_QUOTES, 'UTF-8');

        $allowedTags = '<p><br><strong><b><em><i><u><ul><ol><li><a><h1><h2><h3><h4><h5><h6><span><div><blockquote>';
        $html = strip_tags($html, $allowedTags);

        $html = preg_replace('/(<br\s*\/?>\s*){3,}/i', '<br><br>', $html);
        $html = preg_replace('/<p>\s*<\/p>/i', '', $html);
        // Collapse &nbsp; sequences inside tags
        $html = str_replace("\xC2\xA0", ' ', $html); // UTF-8 non-breaking space
        $html = preg_replace('/\s+/', ' ', $html);
        $html = preg_replace('/<br\s*\/?>/i', '<br>', $html);
        $html = preg_replace('/<\/p>/i', '</p>', $html);

        return trim($html) ?: null;
    }

    /**
     * Parse a datetime string into Y-m-d H:i:s format
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
     * Extract custom field values from the customFieldList array.
     * Returns an associative array keyed by field label.
     */
    protected function extractCustomFields(array $customFieldList): array
    {
        $fields = [];
        foreach ($customFieldList as $field) {
            if (!empty($field['label']) && isset($field['value'])) {
                $label = trim($field['label']);
                $value = trim($field['value']);
                if ($value !== '') {
                    $fields[$label] = $value;
                }
            }
        }
        return $fields;
    }

    /**
     * Parse contact person, email, location, institution, and format
     * from the decoded articleText HTML.
     *
     * The Split API embeds structured info inside the HTML body using
     * patterns like:
     *   "Contact person: Name; email@example.com"
     *   "Location: Some Address"
     *   "Implementing institution: Name"
     *   "Format: Talks/panel discussion"
     */
    protected function parseContactFromHtml(?string $html): array
    {
        $result = [
            'contact'     => null,
            'email'       => null,
            'place'       => null,
            'institution' => null,
            'format'      => null,
        ];

        if (empty($html)) {
            return $result;
        }

        // Work with a plain-text version for easier regex matching
        $text = $this->stripHtml($html);

        if (empty($text)) {
            return $result;
        }

        // ── Email ───────────────────────────────────────────────────────
        // Extract any email address from the text
        if (preg_match('/[\w.+-]+@[\w-]+\.[\w.]+/', $text, $m)) {
            $result['email'] = trim($m[0], '.');
        }

        // ── Contact person ──────────────────────────────────────────────
        // Pattern: "Contact person: Name; email" or "Contact person: Name"
        if (preg_match('/Contact\s+person\s*:\s*([^;\n]+)/i', $text, $m)) {
            $contact = trim($m[1]);
            // Remove email if it leaked into the contact name
            $contact = preg_replace('/[\w.+-]+@[\w-]+\.[\w.]+/', '', $contact);
            $contact = trim($contact, " \t\n\r\0\x0B;,");
            if (!empty($contact)) {
                $result['contact'] = $contact;
            }
        }

        // ── Location ────────────────────────────────────────────────────
        // Pattern: "Location: Address text" (may contain link text)
        if (preg_match('/Location\s*:\s*(.+)/i', $text, $m)) {
            $place = trim($m[1]);
            // Clean up – take only until the next labelled field or end of line
            $place = preg_replace('/(Contact\s+person|Implementing\s+institution|Format)\s*:.*/i', '', $place);
            $place = trim($place, " \t\n\r\0\x0B;,");
            if (!empty($place)) {
                $result['place'] = $place;
            }
        }

        // ── Implementing institution ────────────────────────────────────
        if (preg_match('/Implementing\s+institution\s*:\s*(.+)/i', $text, $m)) {
            $institution = trim($m[1]);
            $institution = preg_replace('/(Location|Contact\s+person|Format)\s*:.*/i', '', $institution);
            $institution = trim($institution, " \t\n\r\0\x0B;,");
            if (!empty($institution)) {
                $result['institution'] = $institution;
            }
        }

        // ── Format ──────────────────────────────────────────────────────
        if (preg_match('/Format\s*:\s*(.+)/i', $text, $m)) {
            $format = trim($m[1]);
            $format = preg_replace('/(Location|Contact\s+person|Implementing\s+institution)\s*:.*/i', '', $format);
            $format = trim($format, " \t\n\r\0\x0B;,");
            if (!empty($format)) {
                $result['format'] = $format;
            }
        }

        return $result;
    }

    /**
     * Extract target-group hints from free-text (description, etc.)
     * Returns an array of hint strings that mapTargetGroups can process.
     */
    protected function extractTargetHints(?string $text): array
    {
        if (empty($text)) {
            return [];
        }

        $hints = [];
        $textLower = strtolower($text);

        $patterns = [
            'children'              => 'children',
            'kids'                  => 'kids',
            'young people'          => 'young',
            'youth'                 => 'youth',
            'teenager'              => 'teenager',
            'students'              => 'students',
            'elderly'               => 'elderly',
            'seniors'               => 'seniors',
            'general public'        => 'public',
            'researchers'           => 'researchers',
            'scientists'            => 'scientists',
            'early career'          => 'early career',
            'phd'                   => 'phd',
            'postdoc'               => 'postdoc',
            'entrepreneurs'         => 'entrepreneurs',
            'business'              => 'business',
            'policy makers'         => 'policy makers',
            'young people (16-26 y)' => 'young',
            'children (0-16 y)'     => 'children',
        ];

        foreach ($patterns as $needle => $hint) {
            if (strpos($textLower, $needle) !== false) {
                $hints[] = $hint;
            }
        }

        return array_unique($hints);
    }

    // =========================================================================
    //  SLUG
    // =========================================================================

    /**
     * Generate unique slug from title and identifier
     */
    protected function generateUniqueSlug(string $title, string $identifier): string
    {
        $slug = str_slug($title);
        $slug = mb_substr($slug, 0, 200);
        $hash = substr(md5($identifier), 0, 8);

        return $slug . '-' . $hash;
    }

    // =========================================================================
    //  COUNTRY
    // =========================================================================

    /**
     * Map country name to country_id
     */
    protected function getCountryId(?string $country): ?int
    {
        if (empty($country)) {
            return null;
        }

        $countryMap = [
            'Croatia'    => 58,
            'Hrvatska'   => 58,
            'HR'         => 58,
            'Germany'    => 85,
            'Deutschland'=> 85,
            'DE'         => 85,
        ];

        foreach ($countryMap as $name => $id) {
            if (strcasecmp($country, $name) === 0) {
                return $id;
            }
        }

        $dbCountry = Db::table('christophheich_calendar_countries')
            ->where('name', 'ILIKE', $country)
            ->first();

        if ($dbCountry) {
            return $dbCountry->id;
        }

        return null;
    }

    // =========================================================================
    //  MAPPING: TARGET GROUPS & THEMES
    // =========================================================================

    /**
     * Map terms to allowed target group values
     */
    protected function mapTargetGroups(array $terms, array $keywords = []): array
    {
        $mapping = [
            // Children
            'children' => 'Children (0-16 y)',
            'kids' => 'Children (0-16 y)',
            'child' => 'Children (0-16 y)',

            // Young people
            'teenager' => 'Young people (16-26 y)',
            'young' => 'Young people (16-26 y)',
            'youth' => 'Young people (16-26 y)',
            'student' => 'Young people (16-26 y)',
            'students' => 'Young people (16-26 y)',
            'talents' => 'Young people (16-26 y)',

            // General public
            'adult' => 'General public/Civil society',
            'public' => 'General public/Civil society',
            'general' => 'General public/Civil society',
            'civil' => 'General public/Civil society',
            'teacher' => 'General public/Civil society',
            'teachers' => 'General public/Civil society',

            // Elderly
            'senior' => 'Elderly people (+65y)',
            'seniors' => 'Elderly people (+65y)',
            'elderly' => 'Elderly people (+65y)',

            // Entrepreneurs
            'entrepreneur' => 'Entrepreneurs/Businesses',
            'entrepreneurs' => 'Entrepreneurs/Businesses',
            'business' => 'Entrepreneurs/Businesses',
            'businesses' => 'Entrepreneurs/Businesses',
            'company' => 'Entrepreneurs/Businesses',
            'companies' => 'Entrepreneurs/Businesses',

            // Policy makers
            'policy' => 'Policy makers',
            'policy maker' => 'Policy makers',
            'policy makers' => 'Policy makers',
            'politician' => 'Policy makers',
            'politicians' => 'Policy makers',
            'government' => 'Policy makers',

            // Early career researchers
            'phd' => 'Early career researchers',
            'postdoc' => 'Early career researchers',
            'postdocs' => 'Early career researchers',
            'early career' => 'Early career researchers',

            // All researchers
            'researcher' => 'All researchers',
            'researchers' => 'All researchers',
            'scientist' => 'All researchers',
            'scientists' => 'All researchers',
            'academic' => 'All researchers',
            'academics' => 'All researchers',
        ];

        $mappedGroups = [];
        $allTerms = array_merge($terms, $keywords);

        foreach ($allTerms as $term) {
            $termLower = strtolower(trim($term));

            if (isset($mapping[$termLower])) {
                $mappedGroups[] = $mapping[$termLower];
                continue;
            }

            foreach ($mapping as $key => $value) {
                if (strpos($termLower, $key) !== false) {
                    $mappedGroups[] = $value;
                    break;
                }
            }
        }

        return array_values(array_unique($mappedGroups));
    }

    /**
     * Map categories and keywords to allowed thematic focuses
     */
    protected function mapThematicFocuses(array $categories, array $keywords = []): array
    {
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
            'sea & water' => 'Sea and water',
            'sea and water' => 'Sea and water',

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
            'science' => 'Talents',
            'discovery' => 'Talents',
            'students' => 'Talents',
            'youth' => 'Talents',
            'skills' => 'Talents',
            'training' => 'Talents',
            'career' => 'Talents',
        ];

        $mappedFocuses = [];
        $allTerms = array_merge($categories, $keywords);

        foreach ($allTerms as $term) {
            $termLower = strtolower(trim($term));

            if (isset($mapping[$termLower])) {
                $mappedFocuses[] = $mapping[$termLower];
                continue;
            }

            foreach ($mapping as $key => $value) {
                if (strpos($termLower, $key) !== false) {
                    $mappedFocuses[] = $value;
                    break;
                }
            }
        }

        $mappedFocuses = array_values(array_unique($mappedFocuses));

        if (empty($mappedFocuses)) {
            return ['Other'];
        }

        return $mappedFocuses;
    }

    // =========================================================================
    //  CATEGORY / IMAGE HELPERS
    // =========================================================================

    /**
     * Attach default category to entry if not already attached
     */
    protected function attachDefaultCategory(int $entryId, int $categoryId = 3): bool
    {
        $pivotTable = 'pensoft_calendar_entries_categories';

        $exists = Db::table($pivotTable)
            ->where('entry_id', $entryId)
            ->where('category_id', $categoryId)
            ->exists();

        if ($exists) {
            return false;
        }

        try {
            Db::table($pivotTable)->insert([
                'entry_id'    => $entryId,
                'category_id' => $categoryId,
            ]);
            return true;
        } catch (\Exception $e) {
            Log::warning('Split Events Import: Failed to attach category', [
                'entry_id'    => $entryId,
                'category_id' => $categoryId,
                'error'       => $e->getMessage(),
            ]);
            return false;
        }
    }

    /**
     * Upload cover image from the Split API item.
     * Checks articleImage, articleDetailImage, and articlegallerymediaData.
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
                return false;
            }
        }

        // Find best image URL
        $imageUrl = null;

        // Priority 1: articleImage
        if (!empty($item['articleImage'])) {
            $imageUrl = $item['articleImage'];
        }

        // Priority 2: articleDetailImage
        if (!$imageUrl && !empty($item['articleDetailImage'])) {
            $imageUrl = $item['articleDetailImage'];
        }

        // Priority 3: first gallery image
        if (!$imageUrl) {
            foreach ($item['articlegallerymediaData'] ?? [] as $media) {
                if (($media['mediaType'] ?? '') === 'Image' && !empty($media['mediaImageUrl'])) {
                    $imageUrl = $media['mediaImageUrl'];
                    break;
                }
                if (!empty($media['mediaData'])) {
                    $imageUrl = $media['mediaData'];
                    break;
                }
            }
        }

        if (!$imageUrl) {
            return false;
        }

        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 30,
                    'header'  => "Accept: image/*\r\n",
                ],
            ]);

            $imageData = file_get_contents($imageUrl, false, $context);

            if ($imageData === false) {
                Log::warning('Split Events Import: Failed to download image', ['url' => $imageUrl]);
                return false;
            }

            $imageName = basename(parse_url($imageUrl, PHP_URL_PATH)) ?: 'cover.jpg';
            $tempPath  = temp_path('import_split_' . uniqid() . '_' . $imageName);
            file_put_contents($tempPath, $imageData);

            $file = new File();
            $file->fromFile($tempPath);
            $file->attachment_type = 'Pensoft\Calendar\Models\Entry';
            $file->attachment_id   = $entryId;
            $file->field           = 'cover_image';
            $file->is_public       = true;
            $file->save();

            $this->fixUploadPermissions($file);

            if (file_exists($tempPath)) {
                unlink($tempPath);
            }

            return true;

        } catch (\Exception $e) {
            Log::error('Split Events Import: Failed to upload cover image', [
                'entry_id' => $entryId,
                'url'      => $imageUrl,
                'error'    => $e->getMessage(),
            ]);
            return false;
        }
    }

    /**
     * Delete existing cover image(s) for an entry from both database and disk storage.
     */
    protected function deleteCoverImage(int $entryId): bool
    {
        $existingImages = Db::table('system_files')
            ->where('attachment_type', 'Pensoft\Calendar\Models\Entry')
            ->where('attachment_id', $entryId)
            ->where('field', 'cover_image')
            ->get();

        if ($existingImages->isEmpty()) {
            return false;
        }

        foreach ($existingImages as $imageRecord) {
            try {
                $file = File::find($imageRecord->id);
                if ($file) {
                    $file->delete();
                } else {
                    Db::table('system_files')->where('id', $imageRecord->id)->delete();
                }
            } catch (\Exception $e) {
                Log::warning('Split Events Import: Failed to delete cover image file', [
                    'file_id'  => $imageRecord->id,
                    'entry_id' => $entryId,
                    'error'    => $e->getMessage(),
                ]);
                Db::table('system_files')->where('id', $imageRecord->id)->delete();
            }
        }

        return true;
    }

    /**
     * Fix file permissions for uploaded files so the web server can generate thumbnails.
     */
    protected function fixUploadPermissions(File $file): void
    {
        try {
            $diskPath    = $file->getDiskPath();
            $uploadsPath = storage_path('app/uploads/public');
            $filePath    = $uploadsPath . '/' . $diskPath;

            if (file_exists($filePath)) {
                chmod($filePath, 0664);

                $dir = dirname($filePath);
                for ($i = 0; $i < 3; $i++) {
                    if ($dir && is_dir($dir) && strpos($dir, $uploadsPath) === 0) {
                        chmod($dir, 0775);
                        $dir = dirname($dir);
                    }
                }

                @chown($filePath, 'www-data');
                @chgrp($filePath, 'www-data');
            }
        } catch (\Exception $e) {
            Log::warning('Split Events Import: Could not fix file permissions', [
                'file_id' => $file->id,
                'error'   => $e->getMessage(),
            ]);
        }
    }

    // =========================================================================
    //  UPDATE MATCHING MODE
    // =========================================================================

    /**
     * Update entries matching by global_id (non-key fields only).
     */
    protected function handleUpdateMatching(string $url, bool $dryRun): int
    {
        $this->info("Starting update matching entries (by global_id / articleId)...");
        $this->info("API URL: {$url}");

        $groupedItems = $this->fetchAndGroupItems($url);
        if ($groupedItems === null) {
            return 1;
        }

        $this->info("Found " . count($groupedItems) . " unique events in API");

        $stats = [
            'updated'   => 0,
            'not_found' => 0,
            'skipped'   => 0,
            'errors'    => 0,
        ];

        $progressBar = $this->output->createProgressBar(count($groupedItems));
        $progressBar->start();

        foreach ($groupedItems as $articleId => $item) {
            $progressBar->advance();

            try {
                if (empty($item['title'])) {
                    $stats['skipped']++;
                    continue;
                }

                $globalId      = $this->buildGlobalId($articleId);
                $matchingEntry = $this->findMatchingEntryByGlobalId($globalId);

                if (!$matchingEntry) {
                    $stats['not_found']++;
                    continue;
                }

                $entry = $this->transformItem($item, $globalId);

                if ($dryRun) {
                    $stats['updated']++;
                    continue;
                }

                // Update non-key fields (keep title, start, end, slug unchanged)
                $updateData = [
                    'global_id'        => $globalId,
                    'identifier'       => $globalId,
                    'description'      => $entry['description'],
                    'url'              => $entry['url'],
                    'place'            => $entry['place'],
                    'country_id'       => $entry['country_id'],
                    'institution'      => $entry['institution'],
                    'contact'          => $entry['contact'],
                    'email'            => $entry['email'],
                    'theme'            => $entry['theme'],
                    'format'           => $entry['format'],
                    'target'           => $entry['target'],
                    'tags'             => $entry['tags'],
                    'fee'              => $entry['fee'],
                    'meta_title'       => $entry['meta_title'],
                    'meta_description' => $entry['meta_description'],
                    'meta_keywords'    => $entry['meta_keywords'],
                    'source'           => $entry['source'],
                    'updated_at'       => Carbon::now(),
                    'deleted_at'       => null,
                ];

                Db::table($this->table)
                    ->where('id', $matchingEntry->id)
                    ->update($updateData);

                $this->uploadCoverImage($matchingEntry->id, $item, false);
                $this->attachDefaultCategory($matchingEntry->id);

                $stats['updated']++;

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing item: ' . ($item['articleId'] ?? 'unknown'));
                $this->error('Message: ' . $e->getMessage());
                Log::error('Split Events Import: Error in update matching', [
                    'articleId' => $item['articleId'] ?? 'unknown',
                    'error'     => $e->getMessage(),
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

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

        Log::info('Split Events Import: Update matching completed', $stats);

        return 0;
    }

    // =========================================================================
    //  UPDATE ALL MATCHING MODE
    // =========================================================================

    /**
     * Update ALL fields for matching entries including title, dates, slug, and cover image.
     */
    protected function handleUpdateAllMatching(string $url, bool $dryRun): int
    {
        $this->info("Starting update ALL fields for matching entries (by global_id / articleId)...");
        $this->info("API URL: {$url}");

        $groupedItems = $this->fetchAndGroupItems($url);
        if ($groupedItems === null) {
            return 1;
        }

        $this->info("Found " . count($groupedItems) . " unique events in API");

        $stats = [
            'updated'   => 0,
            'not_found' => 0,
            'skipped'   => 0,
            'errors'    => 0,
        ];

        $progressBar = $this->output->createProgressBar(count($groupedItems));
        $progressBar->start();

        foreach ($groupedItems as $articleId => $item) {
            $progressBar->advance();

            try {
                if (empty($item['title'])) {
                    $stats['skipped']++;
                    continue;
                }

                $globalId      = $this->buildGlobalId($articleId);
                $matchingEntry = $this->findMatchingEntryByGlobalId($globalId);

                if (!$matchingEntry) {
                    $stats['not_found']++;
                    continue;
                }

                $entry = $this->transformItem($item, $globalId);

                if ($dryRun) {
                    $stats['updated']++;
                    continue;
                }

                $updateData = [
                    'title'            => $entry['title'],
                    'slug'             => $entry['slug'],
                    'global_id'        => $globalId,
                    'identifier'       => $globalId,
                    'start'            => $entry['start'],
                    'end'              => $entry['end'],
                    'all_day'          => $entry['all_day'],
                    'description'      => $entry['description'],
                    'url'              => $entry['url'],
                    'place'            => $entry['place'],
                    'country_id'       => $entry['country_id'],
                    'institution'      => $entry['institution'],
                    'contact'          => $entry['contact'],
                    'email'            => $entry['email'],
                    'theme'            => $entry['theme'],
                    'format'           => $entry['format'],
                    'target'           => $entry['target'],
                    'tags'             => $entry['tags'],
                    'fee'              => $entry['fee'],
                    'meta_title'       => $entry['meta_title'],
                    'meta_description' => $entry['meta_description'],
                    'meta_keywords'    => $entry['meta_keywords'],
                    'is_public'        => $entry['is_public'],
                    'is_internal'      => $entry['is_internal'],
                    'show_on_timeline' => $entry['show_on_timeline'],
                    'source'           => $entry['source'],
                    'updated_at'       => Carbon::now(),
                    'deleted_at'       => null,
                ];

                Db::table($this->table)
                    ->where('id', $matchingEntry->id)
                    ->update($updateData);

                // Delete existing cover image (from DB and disk) and upload new one
                $this->deleteCoverImage($matchingEntry->id);
                $this->uploadCoverImage($matchingEntry->id, $item, true);

                $this->attachDefaultCategory($matchingEntry->id);

                $stats['updated']++;

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing item: ' . ($item['articleId'] ?? 'unknown'));
                $this->error('Message: ' . $e->getMessage());
                Log::error('Split Events Import: Error in update all matching', [
                    'articleId' => $item['articleId'] ?? 'unknown',
                    'error'     => $e->getMessage(),
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

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

        Log::info('Split Events Import: Update all matching completed', $stats);

        return 0;
    }

    // =========================================================================
    //  POPULATE MISSING MODE
    // =========================================================================

    /**
     * Populate missing data for existing entries using the Split API.
     */
    protected function handlePopulateMissing(string $url, bool $dryRun): int
    {
        $this->info("Starting populate missing data (Split.hr)...");
        $this->info("API URL: {$url}");

        // Fetch raw data (not grouped – we need the lookup by articleId)
        try {
            $context = stream_context_create([
                'http' => [
                    'timeout' => 60,
                    'header'  => "Accept: application/json\r\n",
                ],
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

        if (!is_array($data)) {
            $this->error('JSON response is not an array');
            return 1;
        }

        // Build lookup by articleId (keep first occurrence per articleId)
        $apiItems = [];
        foreach ($data as $item) {
            $articleId = $item['articleId'] ?? null;
            if (!empty($articleId) && !isset($apiItems[$articleId])) {
                $apiItems[$articleId] = $item;
            }
        }

        $this->info("Found " . count($apiItems) . " unique items in API");

        // Get existing entries imported from Split
        $existingEntries = Db::table($this->table)
            ->where('source', $this->sourceName)
            ->where('is_internal', false)
            ->where(function ($query) {
                $query->whereNotNull('global_id')
                    ->orWhereNotNull('identifier');
            })
            ->get();

        $this->info("Found " . count($existingEntries) . " existing Split entries");

        $stats = [
            'cover_images_added'    => 0,
            'categories_added'      => 0,
            'countries_updated'     => 0,
            'descriptions_updated'  => 0,
            'urls_updated'          => 0,
            'themes_updated'        => 0,
            'targets_updated'       => 0,
            'contacts_updated'      => 0,
            'emails_updated'        => 0,
            'places_updated'        => 0,
            'institutions_updated'  => 0,
            'formats_updated'       => 0,
            'skipped'               => 0,
            'not_found'             => 0,
            'errors'                => 0,
        ];

        $progressBar = $this->output->createProgressBar(count($existingEntries));
        $progressBar->start();

        foreach ($existingEntries as $entry) {
            $progressBar->advance();

            try {
                $globalId = $entry->global_id ?? $entry->identifier;

                if (empty($globalId)) {
                    $stats['skipped']++;
                    continue;
                }

                // Extract the numeric articleId from the global_id
                $articleId = $globalId;
                if (strpos($articleId, 'split-') === 0) {
                    $articleId = substr($articleId, 6);
                }

                if (!isset($apiItems[$articleId])) {
                    $stats['not_found']++;
                    continue;
                }

                $item    = $apiItems[$articleId];
                $updates = [];

                // Ensure global_id is set
                if (empty($entry->global_id)) {
                    $updates['global_id'] = $this->buildGlobalId($articleId);
                }

                // Cover image
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

                // Category
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

                // Description
                if (empty($entry->description)) {
                    $description = $this->decodeHtmlField($item['articleText'] ?? null);
                    if (empty($description)) {
                        $description = $this->decodeHtmlField($item['summary'] ?? null);
                    }
                    if ($description) {
                        $description = $this->cleanHtml($description);
                    }
                    if (!empty($description)) {
                        $updates['description'] = $description;
                        $stats['descriptions_updated']++;
                    }
                }

                // URL
                if (empty($entry->url)) {
                    $entryUrl = $item['articlelUrl'] ?? null;
                    foreach ($item['articleLinkList'] ?? [] as $link) {
                        if (!empty($link['URL'])) {
                            $entryUrl = $link['URL'];
                            break;
                        }
                    }
                    if (!empty($entryUrl)) {
                        $updates['url'] = mb_substr($entryUrl, 0, 255);
                        $stats['urls_updated']++;
                    }
                }

                // Country
                if (empty($entry->country_id)) {
                    $countryId = $this->getCountryId('Croatia');
                    if (!empty($countryId)) {
                        $updates['country_id'] = $countryId;
                        $stats['countries_updated']++;
                    }
                }

                // Extract customFieldList values
                $customFields = $this->extractCustomFields($item['customFieldList'] ?? []);

                // Theme
                if (empty($entry->theme)) {
                    if (!empty($customFields['Theme'])) {
                        $updates['theme'] = mb_substr($customFields['Theme'], 0, 255);
                        $stats['themes_updated']++;
                    } else {
                        $categoryNames = [];
                        foreach ($item['articleCategories'] ?? [] as $cat) {
                            if (!empty($cat['name'])) {
                                $categoryNames[] = $cat['name'];
                            }
                        }
                        $mappedThemes = $this->mapThematicFocuses($categoryNames, []);
                        if (!empty($mappedThemes)) {
                            $updates['theme'] = mb_substr(implode(', ', $mappedThemes), 0, 255);
                            $stats['themes_updated']++;
                        }
                    }
                }

                // Target
                if (empty($entry->target)) {
                    if (!empty($customFields['Target groups'])) {
                        $updates['target'] = mb_substr($customFields['Target groups'], 0, 255);
                        $stats['targets_updated']++;
                    } else {
                        $description  = $this->decodeHtmlField($item['articleText'] ?? null);
                        $targetTerms  = $this->extractTargetHints($description);
                        $categoryNames = [];
                        foreach ($item['articleCategories'] ?? [] as $cat) {
                            if (!empty($cat['name'])) {
                                $categoryNames[] = $cat['name'];
                            }
                        }
                        $targetGroups = $this->mapTargetGroups(array_merge($categoryNames, $targetTerms), []);
                        if (!empty($targetGroups)) {
                            $updates['target'] = mb_substr(implode(', ', $targetGroups), 0, 255);
                            $stats['targets_updated']++;
                        }
                    }
                }

                // Parse contact info from articleText
                $decodedText = $this->decodeHtmlField($item['articleText'] ?? null);
                $parsedContact = $this->parseContactFromHtml($decodedText);

                // Contact
                if (empty($entry->contact)) {
                    $contact = $parsedContact['contact'] ?? ($item['author'] ?? null);
                    if (!empty($contact)) {
                        $updates['contact'] = mb_substr($contact, 0, 255);
                        $stats['contacts_updated']++;
                    }
                }

                // Email
                if (empty($entry->email) && !empty($parsedContact['email'])) {
                    $updates['email'] = mb_substr($parsedContact['email'], 0, 255);
                    $stats['emails_updated']++;
                }

                // Place
                if (empty($entry->place) && !empty($parsedContact['place'])) {
                    $updates['place'] = mb_substr($parsedContact['place'], 0, 255);
                    $stats['places_updated']++;
                }

                // Institution
                if (empty($entry->institution) && !empty($parsedContact['institution'])) {
                    $updates['institution'] = mb_substr($parsedContact['institution'], 0, 255);
                    $stats['institutions_updated']++;
                }

                // Format
                if (empty($entry->format) && !empty($parsedContact['format'])) {
                    $updates['format'] = mb_substr($parsedContact['format'], 0, 255);
                    $stats['formats_updated']++;
                }

                // Apply updates
                if (!empty($updates) && !$dryRun) {
                    $updates['updated_at'] = Carbon::now();
                    Db::table($this->table)
                        ->where('id', $entry->id)
                        ->update($updates);
                }

                if (empty($updates) && $hasCoverImage && $hasCategory) {
                    $stats['skipped']++;
                }

            } catch (\Exception $e) {
                $stats['errors']++;
                $this->output->writeln('');
                $this->error('Error processing entry ID: ' . $entry->id);
                $this->error('Message: ' . $e->getMessage());
                Log::error('Split Events Import: Error populating missing data', [
                    'entry_id' => $entry->id,
                    'error'    => $e->getMessage(),
                ]);
            }
        }

        $progressBar->finish();
        $this->output->writeln('');
        $this->output->writeln('');

        $this->info("Populate missing data completed!" . ($dryRun ? ' (DRY RUN)' : ''));
        $this->table(
            ['Metric', 'Count'],
            [
                ['Cover images added', $stats['cover_images_added']],
                ['Categories added', $stats['categories_added']],
                ['Countries updated', $stats['countries_updated']],
                ['Descriptions updated', $stats['descriptions_updated']],
                ['URLs updated', $stats['urls_updated']],
                ['Themes updated', $stats['themes_updated']],
                ['Targets updated', $stats['targets_updated']],
                ['Contacts updated', $stats['contacts_updated']],
                ['Emails updated', $stats['emails_updated']],
                ['Places updated', $stats['places_updated']],
                ['Institutions updated', $stats['institutions_updated']],
                ['Formats updated', $stats['formats_updated']],
                ['Skipped (complete)', $stats['skipped']],
                ['Not found in API', $stats['not_found']],
                ['Errors', $stats['errors']],
            ]
        );

        Log::info('Split Events Import: Populate missing completed', $stats);

        return 0;
    }
}
