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
        preprocessor: Type[AbstractPreProcessor]
    ):
    """
    Performs the tests that will be run on all preprocessor report files contained
    in the testing directory.
    Checks:
        - The preprocessor is returning the expected object type and not throwing an error
        - The data contains the required columns for processing in report_processor
    """
    for report, files in files_by_report.items():
        for file in files:
            with open(file, 'rb') as handler:
                file_data = handler.read() 
            file_obj = CommissionFile(file_data=file_data)
            preprocessor_inst = preprocessor(report,99999,file_obj)
            try:
                result = preprocessor_inst.preprocess()
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
    report_names = ['standard'] # TODO add RE Michel
    entity = 'ambro_controls'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, ambro_controls.PreProcessor)

def test_atco_preprocessors():
    report_names = ['standard', 're_michel_pos']
    entity = 'atco'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, atco.PreProcessor)

def test_berry_preprocessors():
    report_names = ['standard', 'baker_pos', 'johnstone_pos', 're_michel_pos', 'united_refrigeration_pos', 'winsupply_pos']
    entity = 'berry'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, berry.PreProcessor)

def test_c_d_valve_preprocessors():
    # python doesn't allow the '&' symbol directly in a name,
    # but we can import a module with the character in it anyway like this
    preprocessor_module = import_module('entities.manufacturers.c&d_valve')
    preprocessor = preprocessor_module.PreProcessor()

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
    report_names = ['paid', 'johnstone_pos']
    entity = 'friedrich'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, friedrich.PreProcessor)

#def test_general_filters_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_genesis_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_glasfloss_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_hardcast_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_jb_ind_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_milwaukee_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_nelco_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_superior_hvacr_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)
#def test_tjernlund_preprocessors():
#    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
#    entity = 'adp'
#    files_by_report = _build_file_listing_by_report(report_names, entity)
#    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)