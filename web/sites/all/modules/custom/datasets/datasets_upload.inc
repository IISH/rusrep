<?php

/**
 * @file
 * Upload dataset files.
 *
 */

/**
 * Upload dataset files
 */
function datasets_upload($form_state) {

  // We will have many fields with the same name, so we need to be able to
  // access the form hierarchically.
  $form['#tree'] = TRUE;

  $form['description'] = array(
    '#type' => 'item',
    '#title' => t('Upload dataset files'),
  );

    $form['file_dataset'] = array(
      '#type' => 'file',
      '#title' => t('Dataset file'),
      '#description' => t('Upload a dataset file, allowed extensions: xls, xlsx'),
    );
    $form['file_paper'] = array(
      '#type' => 'file',
      '#title' => t('Working paper'),
      '#description' => t('Upload a working paper file, allowed extensions: doc, docx'),
    );

    $form['metadata'] = array(
      '#type' => 'fieldset',
      '#title' => t('Metadata'),
      '#collapsible' => TRUE,
      '#collapsed' => FALSE,
    );
    $form['metadata']['provider'] = array(
      '#type' => 'textfield',
      '#title' => t('Provider'),
    );
    $form['metadata']['author'] = array(
      '#type' => 'textfield',
      '#title' => t('Author'),
      '#required' => TRUE,
    );
    $form['metadata']['topic'] = array(
      '#type' => 'select',
      '#title' => t('Topic'),
      '#options' => array(
        'Biological Standards of Living'  => t('Biological Standards of Living'),
        'Demography'                      => t('Demography'),
        'Finance'                         => t('Finance'),
        'Geography'                       => t('Geography'),
        'Geography and Environment'       => t('Geography and Environment'),
        'Human Capital Formation'         => t('Human Capital Formation'),
        'Inequality'                      => t('Inequality'),
        'Institutions'                    => t('Institutions'),
        'Labor Relations'                 => t('Labor Relations'),
        'National Accounts'               => t('National Accounts'),
        'Quality of life'                 => t('Quality of life'),
        'Sustainability'                  => t('Sustainability'),
        'Wages and Prices'                => t('Wages and Prices'),
      ),
      '#required' => TRUE,
    );
    $form['metadata']['indicator'] = array(
      '#type' => 'textfield',
      '#title' => t('Indicator'),
      '#required' => TRUE,
    );
    $form['metadata']['status'] = array(
      '#type' => 'select',
      '#title' => t('Status'),
      '#options' => array(
        0 => t('Private'),
        1 => t('Public'),
      ),
      '#default_value' => 1,
    );
    $form['metadata']['tags'] = array(
      '#type' => 'textfield',
      '#title' => t('Tags'),
    );
    $form['metadata']['version'] = array(
      '#type' => 'select',
      '#title' => t('Version'),
      '#options' => array(
        0 => t('Archive'),
        1 => t('Active'),
      ),
      '#default_value' => 1,
    );
    $form['metadata']['description'] = array(
      '#type' => 'textarea',
      '#title' => t('Description'),
    );

  $form['submit'] = array(
    '#type' => 'submit',
    '#value' => 'Submit',
    '#prefix' => '<div class="submit-warning">'. t('Please be patient after submit, processing the files may take up to one minute to complete.') .'</div>',
  );

  return $form;
}


/**
 * Validate handler for datasets_upload().
 */
function datasets_upload_validate($form, &$form_state) {

  $file_dataset = file_save_upload('file_dataset', array(
    'file_validate_extensions' => array('xls xlsx'), // Validate extensions.
  ));
  $file_paper = file_save_upload('file_paper', array(
    'file_validate_extensions' => array('doc docx'), // Validate extensions.
  ));

  // If the file passed validation:
  if ($file_dataset) {
    // Move the file, into the Drupal file system
    if ($file_dataset = file_move($file_dataset, 'public://')) {
      // Save the file for use in the submit handler.
      $form_state['storage']['file_dataset'] = $file_dataset;
    }
    else {
      form_set_error('file_dataset', t("Failed to write the uploaded dataset file to the site's file folder."));
    }
  }
  else {
    form_set_error('file_dataset', t('No dataset file was uploaded.'));
  }

  // If the file passed validation:
  if ($file_paper) {
    // Move the file, into the Drupal file system
    if ($file_paper = file_move($file_paper, 'public://')) {
      // Save the file for use in the submit handler.
      $form_state['storage']['file_paper'] = $file_paper;
    }
    else {
      form_set_error('file_paper', t("Failed to write the uploaded working paper file to the site's file folder."));
    }
  }
}


/**
 * Submit handler for datasets_upload().
 */
function datasets_upload_submit($form, &$form_state) {
  global $scriptdir;

  $file_dataset = $form_state['storage']['file_dataset'];
  // We are done with the file, remove it from storage.
  unset($form_state['storage']['file_dataset']);
  // Make the storage of the file permanent
  $file_dataset->status = FILE_STATUS_PERMANENT;
  // Save file status.
  file_save($file_dataset);
  $file_dataset_path = drupal_realpath($file_dataset->uri);

  if (isset($form_state['storage']['file_paper'])) {
    $file_paper = $form_state['storage']['file_paper'];
    // We are done with the file, remove it from storage.
    unset($form_state['storage']['file_paper']);
    // Make the storage of the file permanent
    $file_paper->status = FILE_STATUS_PERMANENT;
    // Save file status.
    file_save($file_paper);
    $file_paper_path = drupal_realpath($file_paper->uri);
  }
  else {
    $file_paper_path = '';
  }

  $provider = $form_state['values']['metadata']['provider'];
  $author = $form_state['values']['metadata']['author'];
  $topic  = $form_state['values']['metadata']['topic'];
  $indicator = $form_state['values']['metadata']['indicator'];
  $output = 'test.out';

  $status = `$scriptdir/reader/csv2import.pl -excelfile $file_dataset_path -paper $file_paper_path -provider $provider -author '$author' -indicator '$indicator' -topic '$topic' -output $output`;

  // Set a response to the user.
  if (isset($file_paper)) {
    drupal_set_message(t('The files have been saved: @filename1, @filename2', array('@filename1' => $file_dataset->filename, '@filename2' => $file_paper->filename)));
  }
  else {
    drupal_set_message(t('The file has been saved: @filename1', array('@filename1' => $file_dataset->filename)));
  }

  watchdog('datasets', $status);

}


