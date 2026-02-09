<?php namespace Pensoft\Eventsfields\Components;

use Carbon\Carbon;
use Cms\Classes\ComponentBase;
use Pensoft\Calendar\Models\Entry;

/**
 * Filter Component
 */
class Filter extends ComponentBase
{
    public function componentDetails()
    {
        return [
            'name' => 'Filter Component',
            'description' => 'No description provided yet...'
        ];
    }

    public function defineProperties()
    {
        return [];
    }

    public function onSearchEvents()
    {
        $translator = \RainLab\Translate\Classes\Translator::instance();
        $currentLang = $translator->getLocale();

        $filters = $this->getFilters();
        $page = post('page', 1);

        $this->page['records'] = $this->searchRecords($filters, $page);
        $this->page['exhibitions'] = $this->searchOngoingRecords($filters, 1);
        $this->page['currentLang'] = $currentLang;

        return [
            '#recordsContainer' => $this->renderPartial('events-short-term'),
            '#ongoingEventsContainer' => $this->renderPartial('events-ongoing')
        ];
    }

    public function onLoadOngoingEvents()
    {
        $translator = \RainLab\Translate\Classes\Translator::instance();
        $currentLang = $translator->getLocale();

        $filters = $this->getFilters();
        $page = post('ongoing_page', 1);

        $this->page['exhibitions'] = $this->searchOngoingRecords($filters, $page);
        $this->page['currentLang'] = $currentLang;

        return ['#ongoingEventsContainer' => $this->renderPartial('events-ongoing')];
    }

    protected function getFilters()
    {
        return [
            'searchTerms' => post('searchTerms'),
            'sortCategory' => post('sortCategory'),
            'sortCountry' => post('sortCountry'),
            'sortTarget' => post('sortTarget'),
            'sortTheme' => post('sortTheme'),
            'dateFrom' => post('dateFrom'),
            'dateTo' => post('dateTo'),
        ];
    }

    protected function applyFilters($query, $filters)
    {
        $searchTerms = is_string($filters['searchTerms'])
            ? json_decode($filters['searchTerms'], true)
            : (array)$filters['searchTerms'];

        if (!empty($searchTerms)) {
            $query->where(function($q) use ($searchTerms) {
                foreach ($searchTerms as $term) {
                    $q->orWhere('title', 'ILIKE', "%{$term}%");
                    $q->orWhere('description', 'ILIKE', "%{$term}%");
                    $q->orWhere('institution', 'ILIKE', "%{$term}%");
                    $q->orWhere('fee', 'ILIKE', "%{$term}%");
                    $q->orWhere('tags', 'ILIKE', "%{$term}%");
                    $q->orWhere('contact', 'ILIKE', "%{$term}%");
                    $q->orWhere('email', 'ILIKE', "%{$term}%");
                }
            });
        }

        $query->where('show_on_timeline', false);
        $query->where('is_internal', false);
//        $query->where('end', '>', Carbon::now());

        if ($filters['sortCategory']) {
            $query->byCategory($filters['sortCategory']);
        }

        if ($filters['sortCountry']) {
            $query->where('country_id', "{$filters['sortCountry']}");
        }

        if ($filters['sortTarget']) {
            $query->where('target', 'ilike', "%{$filters['sortTarget']}%");
            $tags = \Db::table('christophheich_calendar_entries')->where('tags', 'ILIKE', "%{$filters['sortTarget']}%");
            $meta_keywords = \Db::table('christophheich_calendar_entries')->where('meta_keywords', 'ILIKE', "%{$filters['sortTarget']}%");
            $query->union($tags);
            $query->union($meta_keywords);
//            $query->orWhere('tags', 'ILIKE', "%{$filters['sortTarget']}%");
//            $query->orWhere('meta_keywords', 'ILIKE', "%{$filters['sortTarget']}%");
        }

        if ($filters['sortTheme']) {
            $query->where('theme', 'ilike', "%{$filters['sortTheme']}%");
            $tags = \Db::table('christophheich_calendar_entries')->where('tags', 'ILIKE', "%{$filters['sortTheme']}%");
            $meta_keywords = \Db::table('christophheich_calendar_entries')->where('meta_keywords', 'ILIKE', "%{$filters['sortTheme']}%");
            $query->union($tags);
            $query->union($meta_keywords);
//            $query->where('tags', 'ILIKE', "%{$filters['sortTheme']}%");
//            $query->orWhere('meta_keywords', 'ILIKE', "%{$filters['sortTheme']}%");
        }

        if ($filters['dateFrom']) {
            $query->where('start', '>=', Carbon::parse($filters['dateFrom']));
        }

        if ($filters['dateTo']) {
            $query->where('end', '<=', Carbon::parse($filters['dateTo']));
        }

        $query->orderBy('start', 'asc');

        return $query;
    }

    protected function searchRecords($filters, $page = 1)
    {
        $query = Entry::query();
        $this->applyFilters($query, $filters);

        // Get all results first, then filter for short-term events (< 8 days duration or no end date)
        $filtered = $query->get()->filter(function ($item) {
            if (is_null($item->end)) {
                return true;
            }
            return Carbon::parse($item->start)->diffInDays(Carbon::parse($item->end)) < 8;
        });

        // Manual pagination
        $perPage = 10;
        $currentPage = $page ?: 1;
        $pagedData = $filtered->slice(($currentPage - 1) * $perPage, $perPage)->values();

        return new \Illuminate\Pagination\LengthAwarePaginator(
            $pagedData,
            $filtered->count(),
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
    }

    protected function searchOngoingRecords($filters, $page = 1)
    {
        $perPage = 10;

        $query = Entry::query();
        $this->applyFilters($query, $filters);

        // Get all results first, then filter for ongoing events (>= 8 days)
        $allOngoingEvents = $query->get()->filter(function ($item) {
            return Carbon::parse($item->start)->diffInDays(Carbon::parse($item->end)) >= 8;
        });

        return new \Illuminate\Pagination\LengthAwarePaginator(
            $allOngoingEvents->forPage($page, $perPage),
            $allOngoingEvents->count(),
            $perPage,
            $page,
            ['path' => request()->url(), 'pageName' => 'ongoing_page']
        );
    }
}
