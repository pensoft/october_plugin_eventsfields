<?php

namespace Pensoft\Eventsfields\Models;

use Model;

/**
 * Settings Model
 */
class Settings extends Model
{
    use \October\Rain\Database\Traits\Validation;

    /**
     * @var array Behaviors implemented by this model.
     */
    public $implement = ['System.Behaviors.SettingsModel'];

    /**
     * @var string Unique code for settings
     */
    public $settingsCode = 'pensoft_eventsfields_settings';

    /**
     * @var string Settings form fields
     */
    public $settingsFields = 'fields.yaml';

    /**
     * @var array Validation rules
     */
    public $rules = [
        'api_url' => 'required|url'
    ];

    /**
     * Initialize default values
     */
    public function initSettingsData()
    {
        $this->api_url = '';
        $this->import_enabled = true;
        $this->import_schedule = 'daily';
    }
}
