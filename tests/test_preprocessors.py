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
FILES = None

def test_adp_preprocessors():
    preprocessor = adp.PreProcessor()
def test_a_gas_preprocessors():
    preprocessor = a_gas.PreProcessor()
def test_air_vent_preprocessors():
    preprocessor = air_vent.PreProcessor()
def test_allied_preprocessors():
    preprocessor = allied.PreProcessor()
def test_ambro_controls_preprocessors():
    preprocessor = ambro_controls.PreProcessor()
def test_atco_preprocessors():
    preprocessor = atco.PreProcessor()
def test_berry_preprocessors():
    preprocessor = berry.PreProcessor()
def test_c_d_valve_preprocessors():
    # python doesn't allow the '&' symbol directly in a name,
    # but we can import a module with the character in it anyway like this
    preprocessor_module = import_module('entities.manufacturers.c&d_valve')
    preprocessor = preprocessor_module.PreProcessor()
def test_cerro_preprocessors():
    preprocessor = cerro.PreProcessor()
def test_clean_comfort_preprocessors():
    preprocessor = clean_comfort.PreProcessor()
def test_famco_preprocessors():
    preprocessor = famco.PreProcessor()
def test_friedrich_preprocessors():
    preprocessor = friedrich.PreProcessor()
def test_general_filters_preprocessors():
    preprocessor = general_filters.PreProcessor()
def test_genesis_preprocessors():
    preprocessor = genesis.PreProcessor()
def test_glasfloss_preprocessors():
    preprocessor = glasfloss.PreProcessor()
def test_hardcast_preprocessors():
    preprocessor = hardcast.PreProcessor()
def test_jb_ind_preprocessors():
    preprocessor = jb_ind.PreProcessor()
def test_milwaukee_preprocessors():
    preprocessor = milwaukee.PreProcessor()
def test_nelco_preprocessors():
    preprocessor = nelco.PreProcessor()
def test_superior_hvacr_preprocessors():
    preprocessor = superior_hvacr.PreProcessor()
def test_tjernlund_preprocessors():
    preprocessor = tjernlund.PreProcessor()