from entities.manufacturers import (
    adp, a_gas, air_vent,
    allied, ambro_controls,
    atco, berry, #c&d_valve,
    cerro, clean_comfort, famco,
    friedrich, general_filters,
    genesis, glasfloss, hardcast,
    jb_ind, milwaukee, nelco,
    superior_hvacr, tjernlund
) 
from importlib import import_module
import os
from entities.commission_file import CommissionFile
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor
from typing import Type
import traceback

FILE_DIRECTORY = './tests/manufacturers'

def _build_file_listing_by_report(reports: list[str], entity: str) -> dict[str,list[str]]:
    """Builds a dictionary with the report names as the keys and a list of file paths as the values
        This will be used to iterate through each file in the directory and test it against its report-specific
        preprocessor"""
    folder_dir = os.path.join(FILE_DIRECTORY, entity)
    report_dirs = {report: os.path.join(folder_dir, report) for report in reports}
    files = {report_name: [
                os.path.join(report_dir, file) for file in os.listdir(report_dir)
            ]
            for report_name, report_dir in report_dirs.items()} 
    return files

def assert_tests_for_each_file(
        files_by_report: dict[str,list[str]],
        entity: str,
        preprocessor: Type[AbstractPreProcessor],
        **kwargs
    ):
    """
    Performs the tests that will be run on all preprocessor report files contained
    in the testing directory.
    Checks:
        - The preprocessor is returning the expected object type and not throwing an error
        - The data contains the required columns for processing in report_processor
    """
    file_password = kwargs.get('file_password', None)
    additionals_dict = {}
    if additonals := files_by_report.pop('additionals', None):
        for file in additonals:
            name = os.path.basename(file).split('.')[0]
            with open(file,'rb') as handler:
                additionals_dict[name] = handler.read()
    for report, files in files_by_report.items():
        for file in files:
            with open(file, 'rb') as handler:
                file_data = handler.read() 
            file_obj = CommissionFile(file_data=file_data, file_password=file_password)
            preprocessor_inst = preprocessor(report,99999,file_obj)
            try:
                filename = os.path.basename(file).split('.')[0]
                additional_file_data = additionals_dict.get(filename,None)
                kwargs['additional_file_1'] = additional_file_data
                result = preprocessor_inst.preprocess(**kwargs)
                tb=''
            except Exception as e:
                result = e
                tb = traceback.format_exc()
                tb = '\n\n'+tb

            msg_prefix = f"""Report '{report}' for '{entity}' with file '{file}'"""
            msg_content = f"failed to return the PreProcessed object.\nInstead it returned or threw {type(result)} with message {str(result)}."
            msg = f'{msg_prefix} {msg_content}{tb}'
            assert isinstance(result, PreProcessedData), msg

            msg_content = f"does not contain required columns: id_string, inv_amt, comm_amt.\nActually contains {result.data.columns.tolist()}"
            assert {'id_string','inv_amt','comm_amt'}.issubset(result.data.columns), msg


def test_adp_preprocessors():
    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
    entity = 'adp'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)

def test_a_gas_preprocessors():
    report_names = ['standard']
    entity = 'a_gas'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, a_gas.PreProcessor)

def test_air_vent_preprocessors():
    report_names = ['standard']
    entity = 'air_vent'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, air_vent.PreProcessor)

def test_allied_preprocessors():
    report_names = ['standard']
    entity = 'allied'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, allied.PreProcessor)

def test_ambro_controls_preprocessors():
    report_names = ['standard', 're_michel_pos']
    entity = 'ambro_controls'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, ambro_controls.PreProcessor)

def test_atco_preprocessors():
    report_names = ['standard', 're_michel_pos']
    entity = 'atco'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, atco.PreProcessor)

def test_berry_preprocessors():
    report_names = ['standard', 'baker_pos', 'johnstone_pos', 're_michel_pos',
                     'united_refrigeration_pos', 'winsupply_pos']
    entity = 'berry'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, berry.PreProcessor)

def test_c_d_valve_preprocessors():
    # python doesn't allow the '&' symbol directly in a name,
    # but we can import a module with the character in it anyway like this
    preprocessor_module = import_module('entities.manufacturers.c&d_valve')
    report_names = ['standard', 'baker', 'johnstone', 'additionals']
    entity = 'c&d_valve'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, preprocessor_module.PreProcessor)

def test_cerro_preprocessors():
    report_names = ['standard']
    entity = 'cerro'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, cerro.PreProcessor)

def test_clean_comfort_preprocessors():
    report_names = ['standard'] # TODO add prostat
    entity = 'clean_comfort'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, clean_comfort.PreProcessor)

def test_famco_preprocessors():
    report_names = ['standard', 'johnstone_pos']
    entity = 'famco'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, famco.PreProcessor)

def test_friedrich_preprocessors():
    report_names = ['paid', 'johnstone_pos', 'ferguson_pos']
    entity = 'friedrich'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, friedrich.PreProcessor)

def test_general_filters_preprocessors():
    report_names = ['standard', 'unifilter']
    entity = 'general_filters'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, general_filters.PreProcessor)

def test_genesis_preprocessors():
    report_names = ['sales_detail', 'baker_pos', 'lennox_pos', 'winsupply_pos', 'rebate_detail']
    entity = 'genesis'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, genesis.PreProcessor)

def test_glasfloss_preprocessors():
    report_names = ['standard']
    entity = 'glasfloss'
    file_password = '013084'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, glasfloss.PreProcessor, file_password=file_password)

def test_hardcast_preprocessors():
    report_names = ['standard']
    entity = 'hardcast'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, hardcast.PreProcessor)

def test_jb_ind_preprocessors():
    report_names = ['standard'] 
    entity = 'jb_ind'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, jb_ind.PreProcessor)

def test_milwaukee_preprocessors():
    report_names = ['full_detail_list']
    entity = 'milwaukee'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, milwaukee.PreProcessor)

def test_nelco_preprocessors():
    report_names = ['standard']
    entity = 'nelco'
    comm_rate = 0.50 
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, nelco.PreProcessor, standard_commission_rate=comm_rate)

def test_superior_hvacr_preprocessors():
    report_names = ['standard', 'uri_report', 'johnstone', 'additionals']
    entity = 'superior_hvacr'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, superior_hvacr.PreProcessor)

def test_tjernlund_preprocessors():
    report_names = ['standard']
    entity = 'tjernlund'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, tjernlund.PreProcessor)