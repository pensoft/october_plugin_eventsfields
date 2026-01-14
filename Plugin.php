<?php namespace Pensoft\EventsFields;

use Backend;
use Pensoft\Calendar\Controllers\Entries;
use Pensoft\Calendar\Models\Entry;
use Pensoft\Eventsfields\Components\Filter;
use RainLab\Location\Models\Country;
use System\Classes\PluginBase;
use PhpOffice\PhpSpreadsheet\IOFactory;
use PhpOffice\PhpSpreadsheet\Shared\Date as ExcelDate;
use ApplicationException;
use Event;
use Flash;

class Plugin extends PluginBase
{

    public $require = [
        'pensoft.calendar',
    ];

    public function boot()
    {
        // Add database columns
        $this->addDatabaseColumns();

        // Extend model fillable
        $this->extendModelFillable();

        // Extend model relations
        $this->extendModelRelations();

        // Extend form fields
        $this->extendFormFields();

        // Extend model with search
        $this->extendModelSearch();

        // Extend the Events controller with import actions
        $this->extendEventsController();

        // Extend list toolbar
        $this->extendListToolbar();
    }

    /**
     * Add database columns if they don't exist
     */
    protected function addDatabaseColumns()
    {
        $tableName = 'christophheich_calendar_entries';

        if (!\Schema::hasTable($tableName)) {
            return;
        }

        $columns = [
            'is_public' => function ($table) {
                $table->boolean('is_public')->default(true);
            },
            'institution' => function ($table) {
                $table->string('institution')->nullable();
            },
            'country_id' => function ($table) {
                $table->integer('country_id')->unsigned()->nullable();
            },
            'target' => function ($table) {
                $table->text('target')->nullable();
            },
            'theme' => function ($table) {
                $table->text('theme')->nullable();
            },
            'format' => function ($table) {
                $table->text('format')->nullable();
            },
            'fee' => function ($table) {
                $table->string('fee')->nullable();
            },
            'remarks' => function ($table) {
                $table->text('remarks')->nullable();
            },
            'tags' => function ($table) {
                $table->text('tags')->nullable();
            },
            'contact' => function ($table) {
                $table->string('contact')->nullable();
            },
            'email' => function ($table) {
                $table->string('email')->nullable();
            },
        ];

        // Drop old 'country' string column if exists
        if (\Schema::hasColumn($tableName, 'country') && !\Schema::hasColumn($tableName, 'country_id')) {
            \Schema::table($tableName, function ($table) {
                $table->dropColumn('country');
            });
        }

        foreach ($columns as $column => $callback) {
            if (!\Schema::hasColumn($tableName, $column)) {
                \Schema::table($tableName, $callback);
            }
        }
    }

    /**
     * Extend Entry model fillable property
     */
    protected function extendModelFillable()
    {
        Entry::extend(function ($model) {
            $newFillable = [
                'start',
                'end',
                'title',
                'slug',
                'description',
                'place',
                'url',
                'institution',
                'is_public',
                'country_id',
                'target',
                'theme',
                'format',
                'fee',
                'remarks',
                'tags',
                'contact',
                'email',
            ];

            if (method_exists($model, 'addFillable')) {
                $model->addFillable($newFillable);
            } else {
                $model->fillable = array_unique(array_merge($model->fillable ?? [], $newFillable));
            }

            // Handle empty strings for integer/nullable fields before save
            $model->bindEvent('model.beforeSave', function () use ($model) {
                // Convert empty strings to null for integer fields
                $integerFields = ['country_id'];
                foreach ($integerFields as $field) {
                    if ($model->{$field} === '' || $model->{$field} === null) {
                        $model->{$field} = null;
                    }
                }

                // Convert empty strings to null for nullable text fields
                $nullableFields = [
                    'institution', 'target', 'theme', 'format', 'fee',
                    'remarks', 'tags', 'contact', 'email',
                    'meta_keywords', 'meta_description', 'meta_title'
                ];
                foreach ($nullableFields as $field) {
                    if (isset($model->{$field}) && $model->{$field} === '') {
                        $model->{$field} = null;
                    }
                }
            });
        });
    }

    protected function extendModelRelations()
    {
        Entry::extend(function ($model) {
            // belongsTo relation for country
            if (!isset($model->belongsTo['country'])) {
                $model->belongsTo['country'] = [
                    'RainLab\Location\Models\Country',
                    'key' => 'country_id'
                ];
            }

            // belongsToMany relation for categories
            if (!isset($model->belongsToMany['categories'])) {
                $model->belongsToMany['categories'] = [
                    'Pensoft\Calendar\Models\Category',
                    'table' => 'pensoft_calendar_entries_categories',
                    'key' => 'entry_id',
                    'otherKey' => 'category_id'
                ];
            }
        });
    }

    /**
     * Extend form fields
     */
    protected function extendFormFields()
    {
        if (!class_exists('\Pensoft\Calendar\Controllers\Entries')) {
            return;
        }

        Entries::extendFormFields(function ($form, $model, $context) {
            // Only extend for Entry model
            if (!$model instanceof Entry) {
                return;
            }

            $form->addTabFields([
                'institution' => [
                    'label' => 'Implementing institution (full name, no acronym)',
                    'span'  => 'left',
                    'type'  => 'text',
                    'tab'   => 'Additional fields',
                ],
                'is_public' => [
                    'label'   => 'Public or closed event (by invitation)',
                    'span'    => 'right',
                    'type'    => 'switch',
                    'default' => true,
                    'comment' => 'ON -> public; OFF -> closed',
                    'tab'     => 'Additional fields',
                ],
                'country_id' => [
                    'label'       => 'Country',
                    'span'        => 'left',
                    'type'        => 'dropdown',
                    'emptyOption' => '-- Select Country --',
                    'tab'         => 'Additional fields',
                ],
                'target' => [
                    'label'      => 'Target',
                    'mode'       => 'string',
                    'separator'  => 'comma',
                    'customTags' => true,
                    'useKey'     => false,
                    'span'       => 'right',
                    'required'   => false,
                    'type'       => 'taglist',
                    'tab'        => 'Additional fields',
                ],
                'theme' => [
                    'label' => 'Thematic focus',
                    'span'  => 'left',
                    'type'  => 'text',
                    'tab'   => 'Additional fields',
                ],
                'format' => [
                    'label' => 'Format/Category',
                    'span'  => 'right',
                    'type'  => 'text',
                    'tab'   => 'Additional fields',
                ],
                'fee' => [
                    'label' => 'Entrance fee',
                    'span'  => 'left',
                    'type'  => 'text',
                    'tab'   => 'Additional fields',
                ],
                'remarks' => [
                    'label' => 'Remarks',
                    'span'  => 'right',
                    'type'  => 'richeditor',
                    'size'  => 'small',
                    'tab'   => 'Additional fields',
                ],
                'tags' => [
                    'label'      => 'TAGs',
                    'mode'       => 'string',
                    'separator'  => 'space',
                    'customTags' => true,
                    'useKey'     => false,
                    'span'       => 'left',
                    'required'   => false,
                    'type'       => 'taglist',
                    'tab'        => 'Additional fields',
                ],
                'contact' => [
                    'label' => 'Contact person',
                    'span'  => 'left',
                    'type'  => 'text',
                    'tab'   => 'Additional fields',
                ],
                'email' => [
                    'label' => 'Email contact',
                    'span'  => 'right',
                    'type'  => 'text',
                    'tab'   => 'Additional fields',
                ],
            ]);
        });

        // Add dropdown options method to Entry model
        Entry::extend(function ($model) {
            $model->addDynamicMethod('getCountryIdOptions', function () {
                return ['' => '-- Select Country --'] + Country::isEnabled()->orderBy('name')->lists('name', 'id');
            });
        });
    }

    /**
     * Extend model with search functionality
     */
    protected function extendModelSearch()
    {
        Entry::extend(function ($model) {
            $model->addDynamicMethod('scopeSearchEvents', function ($query, $searchTerms) {
                if (!empty($searchTerms) && is_array($searchTerms)) {
                    foreach ($searchTerms as $term) {
                        $query->orWhere('title', 'ILIKE', "%{$term}%");
                        $query->orWhere('description', 'ILIKE', "%{$term}%");
                        $query->orWhere('institution', 'ILIKE', "%{$term}%");
                        $query->orWhere('fee', 'ILIKE', "%{$term}%");
                        $query->orWhere('tags', 'ILIKE', "%{$term}%");
                        $query->orWhere('contact', 'ILIKE', "%{$term}%");
                        $query->orWhere('email', 'ILIKE', "%{$term}%");
                    }
                }
                return $query;
            });

            $model->addDynamicMethod('onSearchEvents', function () use ($model) {
                $searchTerms = post('searchTerms');
                $sortCategory = post('sortCategory');
                $sortCountry = post('sortCountry');
                $sortTarget = post('sortTarget');
                $sortTheme = post('sortTheme');
                $dateFrom = post('dateFrom');
                $dateTo = post('dateTo');
                $model->page['records'] = $model->searchEvents($searchTerms, $sortCategory, $sortCountry, $sortTarget, $sortTheme, $dateFrom, $dateTo);
                return ['#recordsContainer' => $model->renderPartial('eventslist')];
            });
        });
    }

    /**
     * Extend Events controller with import actions
     */
    /**
     * Extend Events controller with import actions
     */
    protected function extendEventsController()
    {
        Entries::extend(function ($controller) {

            $controller->addDynamicMethod('import_excel', function () use ($controller) {
                $controller->pageTitle = 'Import Events from Excel';
                $controller->bodyClass = 'compact-container';

                // Pass countries to the partial
                $controller->vars['countries'] = Country::isEnabled()->orderBy('name')->lists('name', 'id');

                return $controller->makePartial('$/pensoft/eventsfields/partials/_import_excel.htm');
            });

            $controller->addDynamicMethod('onImportExcel', function () use ($controller) {

                $file = null;

                if (request()->hasFile('excel_file')) {
                    $file = request()->file('excel_file');
                } elseif (isset($_FILES['excel_file']) && $_FILES['excel_file']['error'] === UPLOAD_ERR_OK) {
                    $file = $_FILES['excel_file'];
                }

                if (!$file) {
                    throw new ApplicationException('Please select an Excel file to import.');
                }

                if (is_array($file)) {
                    $filePath = $file['tmp_name'];
                    $extension = strtolower(pathinfo($file['name'], PATHINFO_EXTENSION));
                } else {
                    $filePath = $file->getRealPath();
                    $extension = strtolower($file->getClientOriginalExtension());
                }

                $allowedExtensions = ['xlsx', 'xls'];
                if (!in_array($extension, $allowedExtensions)) {
                    throw new ApplicationException('Invalid file type. Please upload an Excel file (.xlsx or .xls).');
                }

                $sheetName = trim(post('sheet_name', 'Upload sheet'));
                $countryId = post('country_id');
                $categoryIds = post('categories', []);

                if (empty($sheetName)) {
                    $sheetName = 'Upload sheet';
                }

                // Validate required country
                if (empty($countryId)) {
                    throw new ApplicationException('Please select a country.');
                }

                $results = Plugin::performImport($filePath, $sheetName, $categoryIds, $countryId);

                $message = "Import completed: {$results['success']} events imported.";

                if ($results['skipped'] > 0) {
                    $message .= " {$results['skipped']} duplicates skipped.";
                }

                if ($results['failed'] > 0) {
                    $message .= " {$results['failed']} rows failed.";
                }

                if (!empty($results['errors'])) {
                    $errorMessages = implode("\n", array_slice($results['errors'], 0, 10));
                    if (count($results['errors']) > 10) {
                        $errorMessages .= "\n... and " . (count($results['errors']) - 10) . " more errors.";
                    }
                    Flash::warning($message . "\n\nErrors:\n" . $errorMessages);
                } else {
                    Flash::success($message);
                }

                return Backend::redirect('pensoft/calendar/entries');
            });
        });
    }

    /**
     * Extend list toolbar to add import button
     */
    protected function extendListToolbar()
    {
        Event::listen('backend.menu.extendItems', function ($manager) {
            $manager->addSideMenuItems('Pensoft.Calendar', 'main-menu-item', [
                'import_excel' => [
                    'label'       => 'Import from Excel',
                    'icon'        => 'icon-upload',
                    'code'        => 'entries_fields_import_from_excel',
                    'owner'       => 'Pensoft.Calendar',
                    'url'         => Backend::url('pensoft/calendar/entries/import_excel'),
                    'permissions' => ['pensoft.calendar.*']
                ]
            ]);
        });
    }

    /**
     * Perform the actual import
     */
    /**
     * Perform the actual import
     */
    public static function performImport($filePath, $sheetName = 'Upload sheet', $categoryIds = [], $countryId = null)
    {
        $results = [
            'success'  => 0,
            'failed'   => 0,
            'skipped'  => 0,
            'errors'   => []
        ];

        try {
            $spreadsheet = IOFactory::load($filePath);

            $worksheet = $spreadsheet->getSheetByName($sheetName);

            if (!$worksheet) {
                $trySheets = ['Upload sheet', 'Sheet1', 'Data', 'Events'];
                foreach ($trySheets as $trySheet) {
                    $worksheet = $spreadsheet->getSheetByName($trySheet);
                    if ($worksheet) {
                        break;
                    }
                }
            }

            if (!$worksheet) {
                $sheetNames = $spreadsheet->getSheetNames();
                foreach ($sheetNames as $name) {
                    if (stripos($name, 'read') === false) {
                        $worksheet = $spreadsheet->getSheetByName($name);
                        break;
                    }
                }
            }

            if (!$worksheet) {
                $availableSheets = implode(', ', $spreadsheet->getSheetNames());
                throw new ApplicationException("Sheet not found. Available sheets: {$availableSheets}");
            }

            $highestRow = $worksheet->getHighestRow();
            $highestColumn = $worksheet->getHighestColumn();

            $headers = [];
            $headerRow = $worksheet->rangeToArray('A1:' . $highestColumn . '1', null, true, false)[0];
            foreach ($headerRow as $index => $header) {
                $headers[$index] = self::normalizeHeader($header);
            }

            for ($row = 2; $row <= $highestRow; $row++) {
                $rowData = $worksheet->rangeToArray('A' . $row . ':' . $highestColumn . $row, null, true, false)[0];

                try {
                    $eventData = self::mapRowToEvent($rowData, $headers);

                    if (empty($eventData['title']) && empty($eventData['start'])) {
                        continue;
                    }

                    // Add country_id from form selection
                    if (!empty($countryId)) {
                        $eventData['country_id'] = $countryId;
                    }

                    // Check for duplicate entry
                    if (self::isDuplicateEntry($eventData)) {
                        $results['skipped']++;
                        continue;
                    }

                    // Create entry
                    $entry = new Entry();
                    foreach ($eventData as $key => $value) {
                        if ($value !== null && $value !== '') {
                            $entry->{$key} = $value;
                        }
                    }
                    $entry->show_on_timeline = false;
                    $entry->is_internal = false;
                    $entry->save();

                    // Attach categories if provided
                    if (!empty($categoryIds)) {
                        $entry->categories()->sync($categoryIds);
                    }

                    $results['success']++;

                } catch (\Exception $e) {
                    $results['failed']++;
                    $results['errors'][] = "Row {$row}: " . $e->getMessage();
                }
            }

        } catch (\Exception $e) {
            throw new ApplicationException("Error importing Excel file: " . $e->getMessage());
        }

        return $results;
    }

    /**
     * Check if entry already exists based on title, start, end, place, country
     */
    /**
     * Check if entry already exists based on title, start date, end date, place, country
     * Note: Only compares date portion of start/end timestamps, not the time
     */
    protected static function isDuplicateEntry($eventData)
    {
        $query = Entry::query();

        // Check title
        if (!empty($eventData['title'])) {
            $query->where('title', $eventData['title']);
        } else {
            $query->whereNull('title');
        }

        // Check start (date only, ignore time)
        if (!empty($eventData['start'])) {
            $startDate = date('Y-m-d', strtotime($eventData['start']));
            $query->whereRaw('DATE(start) = ?', [$startDate]);
        } else {
            $query->whereNull('start');
        }

        // Check end (date only, ignore time)
        if (!empty($eventData['end'])) {
            $endDate = date('Y-m-d', strtotime($eventData['end']));
            $query->whereRaw('DATE("end") = ?', [$endDate]);
        } else {
            $query->whereNull('end');
        }

        // Check place
//        if (!empty($eventData['place'])) {
//            $query->where('place', $eventData['place']);
//        } else {
//            $query->whereNull('place');
//        }

//        // Check country_id
//        if (!empty($eventData['country_id'])) {
//            $query->where('country_id', $eventData['country_id']);
//        } else {
//            $query->whereNull('country_id');
//        }

        return $query->exists();
    }

    /**
     * Map Excel row data to event fields
     */
    protected static function mapRowToEvent($rowData, $headers)
    {
        $data = [];
        $targetGroups = [];

        $startDate = null;
        $startTime = null;
        $endDate = null;
        $endTime = null;

        foreach ($headers as $index => $header) {
            if ($header === null) {
                continue;
            }

            $value = $rowData[$index] ?? null;

            if (empty($value) && $value !== 0 && $value !== '0') {
                continue;
            }

            switch ($header) {
                case 'start_date':
                    $startDate = self::parseExcelDate($value);
                    break;

                case 'end_date':
                    $endDate = self::parseExcelDate($value);
                    break;

                case 'start_time':
                    $startTime = self::parseExcelTime($value);
                    break;

                case 'end_time':
                    $endTime = self::parseExcelTime($value);
                    break;

                case 'address':
                    $data['place'] = $value;
                    break;

                case 'title':
                    $data['title'] = $value;
                    break;

                case 'institution':
                    $data['institution'] = $value;
                    break;

                case 'description':
                    $data['description'] = $value;
                    break;

                case 'links':
                    $data['url'] = $value;
                    break;

                case 'target_group1':
                case 'target_group2':
                case 'target_group3':
                    if (!empty($value)) {
                        $targetGroups[] = $value;
                    }
                    break;

                case 'theme':
                    $data['theme'] = $value;
                    break;

                case 'format':
                    $data['format'] = $value;
                    break;

                case 'is_public':
                    $data['is_public'] = self::parsePublicStatus($value);
                    break;

                case 'fee':
                    $data['fee'] = $value;
                    break;

                case 'remarks':
                    $data['remarks'] = $value;
                    break;

                case 'contact':
                    $data['contact'] = $value;
                    break;

                case 'email':
                    $data['email'] = $value;
                    break;

                case 'tags':
                    $data['tags'] = $value;
                    break;
            }
        }

        // Generate slug from title
        if (!empty($data['title'])) {
            $data['slug'] = self::generateUniqueSlug($data['title']);
        }

        // Combine date and time into start timestamp
        if ($startDate) {
            $startDateTime = $startDate;
            if ($startTime) {
                $startDateTime .= ' ' . $startTime;
            } else {
                $startDateTime .= ' 00:00:00';
            }
            $data['start'] = $startDateTime;
        }

        // Combine end date and time into end timestamp
        if ($endDate) {
            $endDateTime = $endDate;
            if ($endTime) {
                $endDateTime .= ' ' . $endTime;
            } else {
                $endDateTime .= ' 23:59:59';
            }
            $data['end'] = $endDateTime;
        } elseif ($startDate) {
            $endDateTime = $startDate;
            if ($endTime) {
                $endDateTime .= ' ' . $endTime;
            } else {
                $endDateTime .= ' 23:59:59';
            }
            $data['end'] = $endDateTime;
        }

        // Combine target groups
        if (!empty($targetGroups)) {
            $data['target'] = implode(', ', $targetGroups);
        }

        return $data;
    }

    /**
     * Generate unique slug from title
     */
    protected static function generateUniqueSlug($title)
    {
        $slug = \Str::slug($title);
        $slug = substr($slug, 0, 200);
        $originalSlug = $slug;
        $counter = 1;

        while (Entry::where('slug', $slug)->exists()) {
            $slug = $originalSlug . '-' . $counter;
            $counter++;
        }

        return $slug;
    }

    /**
     * Normalize header names to match database fields
     */
    protected static function normalizeHeader($header)
    {
        if (empty($header)) {
            return null;
        }

        $header = strtolower(trim($header));

        $mapping = [
            'date'                                              => 'start_date',
            'end date (if multi-day)'                           => 'end_date',
            'start (hour)'                                      => 'start_time',
            'end (hour)'                                        => 'end_time',
            'adress (concrete adress)'                          => 'address',
            'address (concrete address)'                        => 'address',
            'title of event (english)'                          => 'title',
            'implementing institution (full name, no acronym)'  => 'institution',
            'short description (english)'                       => 'description',
            'links'                                             => 'links',
            'target group1'                                     => 'target_group1',
            'target group2 (optional)'                          => 'target_group2',
            'target group3 (optional)'                          => 'target_group3',
            'thematic focus'                                    => 'theme',
            'format/category'                                   => 'format',
            'public or closed event (by invitation)'            => 'is_public',
            'entrance fee'                                      => 'fee',
            'remarks'                                           => 'remarks',
            'contact person'                                    => 'contact',
            'email contact'                                     => 'email',
            'picture for calendar'                              => 'picture',
            'tags'                                              => 'tags',
        ];

        return $mapping[$header] ?? null;
    }

    /**
     * Parse public/closed status to boolean
     */
    protected static function parsePublicStatus($value)
    {
        if (empty($value)) {
            return true;
        }

        $value = strtolower(trim($value));
        $closedValues = ['closed', 'closed (invitation /registration)', 'invitation', 'private', 'no', 'false', '0'];

        return !in_array($value, $closedValues);
    }

    /**
     * Parse Excel date value
     */
    protected static function parseExcelDate($value)
    {
        if (empty($value)) {
            return null;
        }

        if (is_string($value) && preg_match('/^\d{4}-\d{2}-\d{2}/', $value)) {
            return date('Y-m-d', strtotime($value));
        }

        if (is_numeric($value)) {
            try {
                $dateTime = ExcelDate::excelToDateTimeObject($value);
                return $dateTime->format('Y-m-d');
            } catch (\Exception $e) {
                return null;
            }
        }

        try {
            $timestamp = strtotime($value);
            if ($timestamp !== false) {
                return date('Y-m-d', $timestamp);
            }
        } catch (\Exception $e) {
            return null;
        }

        return null;
    }

    /**
     * Parse Excel time value
     */
    protected static function parseExcelTime($value)
    {
        if (empty($value)) {
            return null;
        }

        if (is_string($value) && preg_match('/^\d{2}:\d{2}(:\d{2})?$/', $value)) {
            return $value;
        }

        if (is_numeric($value) && $value >= 0 && $value < 1) {
            $totalSeconds = round($value * 86400);
            $hours = floor($totalSeconds / 3600);
            $minutes = floor(($totalSeconds % 3600) / 60);
            $seconds = $totalSeconds % 60;
            return sprintf('%02d:%02d:%02d', $hours, $minutes, $seconds);
        }

        if (is_numeric($value)) {
            try {
                $dateTime = ExcelDate::excelToDateTimeObject($value);
                return $dateTime->format('H:i:s');
            } catch (\Exception $e) {
                return null;
            }
        }

        return null;
    }

    public function registerComponents()
    {
        return [
            Filter::class => 'Filter'
        ];
    }

    public function registerSettings()
    {
    }
}
