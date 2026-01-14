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

    public function onSearchEvents() {
        $translator = \RainLab\Translate\Classes\Translator::instance();

        $currentLang = $translator->getLocale();

        $searchTerms = post('searchTerms');
        $sortCategory = post('sortCategory');
        $sortCountry = post('sortCountry');
        $sortTarget = post('sortTarget');
        $sortTheme = post('sortTheme');
        $dateFrom = post('dateFrom');
        $dateTo = post('dateTo');
        $page = post('page', 1);
        $this->page['records'] = $this->searchRecords($searchTerms, $sortCategory, $sortCountry, $sortTarget, $sortTheme, $dateFrom, $dateTo, $page);
        $this->page['currentLang'] = $currentLang;
        return ['#recordsContainer' => $this->renderPartial('events-short-term')];
    }

    protected function searchRecords(
        $searchTerms = '',
        $sortCategory = 0,
        $sortCountry = 0,
        $sortTarget = 0,
        $sortTheme = 0,
        $dateFrom = '',
        $dateTo = '',
        $page = 1
    ) {
        $searchTerms = is_string($searchTerms) ? json_decode($searchTerms, true) : (array)$searchTerms;
        $result = Entry::searchEvents($searchTerms);

        $result->where('show_on_timeline', false);
        $result->where('is_internal', false);

        if($sortCategory){
            $result->byCategory($sortCategory);
        }

        if($sortCountry){
            $result->where('country_id', "{$sortCountry}");
        }

        if($sortTarget){
            $result->where('target', 'ilike', "%{$sortTarget}%");
        }
        if($sortTheme){
            $result->where('theme', 'ilike', "%{$sortTheme}%");
        }
        if($dateFrom){
            $result->where('start', '>=', Carbon::parse($dateFrom));
        }
        if($dateTo){
            $result->where('end', '<=', Carbon::parse($dateTo));
        }

        $result->orderBy('start', 'asc');

        return $result->paginate(4, $page);
    }
}
