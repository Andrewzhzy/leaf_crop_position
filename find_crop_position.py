from datetime import date, timedelta
from terra_common import CoordinateConverter as CC
import os, logging, traceback, time, utils, json
import skimage.io as sio
import pickle
from plyfile import PlyData
import multiprocessing


def find_crop_position(raw_data_folder, ply_data_folder, cc_path='./coord_convert.pkl', log_lv=logging.INFO):
    with open(cc_path, 'rb') as f:
        cc = pickle.load(f)
    raw_data_folder = os.path.join(raw_data_folder, '')
    ply_data_folder = os.path.join(ply_data_folder, '')
    cpname = multiprocessing.current_process().name
    logger = logging.getLogger('ppln_' + os.path.basename(os.path.dirname(raw_data_folder)) + '_' + cpname)
    logger.setLevel(log_lv)
    formatter = logging.Formatter('%(asctime)s - %(name)s %(levelname)s:\t%(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(log_lv)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.info('Start processing')
    # get file name
    for filename in os.listdir(raw_data_folder):
        if 'east' + '_0_g.png' in filename:
            east_gIm_name = filename
        if 'west' + '_0_g.png' in filename:
            west_gIm_name = filename
        if 'east' + '_0_p.png' in filename:
            east_pIm_name = filename
        if 'west' + '_0_p.png' in filename:
            west_pIm_name = filename
        if 'metadata.json' in filename:
            json_name = filename
    # read png
    try:
        east_gIm = sio.imread(os.path.join(raw_data_folder, east_gIm_name))
        east_pIm = sio.imread(os.path.join(raw_data_folder, east_pIm_name))
        west_gIm = sio.imread(os.path.join(raw_data_folder, west_gIm_name))
        west_pIm = sio.imread(os.path.join(raw_data_folder, west_pIm_name))
    except:
        logger.error('Image reading error! Skip.')
        return -1

    # check existence of ply file
    east_ply_data_path = None
    west_ply_data_path = None
    for filename in os.listdir(ply_data_folder):
        if 'east' in filename:
            east_ply_data_path = os.path.expanduser(os.path.join(ply_data_folder, filename))
        if 'west' in filename:
            west_ply_data_path = os.path.expanduser(os.path.join(ply_data_folder, filename))
    if east_ply_data_path is None or west_ply_data_path is None:
        logger.error('ply file does not exist. path:{}'.format(ply_data_folder))
        return -1
    pass
    # read ply
    try:
        east_ply_data = PlyData.read(east_ply_data_path)
        west_ply_data = PlyData.read(west_ply_data_path)
    except:
        logger.error('ply file reading error! Skip. file_path:{}'.format(ply_data_folder))
        return -1
    # read json
    try:
        with open(os.path.join(raw_data_folder, json_name), 'r') as json_f:
            json_data = json.load(json_f)
        east_json_info = utils.get_json_info(json_data, sensor='east')
        west_json_info = utils.get_json_info(json_data, sensor='west')
    except:
        logger.error('Load json file unsuccessful.')
        return -4
    # offset
    east_ply_data = utils.ply_offset(east_ply_data, east_json_info)
    west_ply_data = utils.ply_offset(west_ply_data, west_json_info)
    # ply to xyz
    east_ply_xyz_map = utils.ply2xyz(east_ply_data, east_pIm, east_gIm)
    west_ply_xyz_map = utils.ply2xyz(west_ply_data, west_pIm, west_gIm)
    # crop position
    east_crop_position_dict = utils.depth_crop_position(east_ply_xyz_map, cc)
    west_crop_position_dict = utils.depth_crop_position(west_ply_xyz_map, cc)
    # save to fle
    folder_name = os.path.basename(os.path.dirname(raw_data_folder))
    east_pkl_path = os.path.join('./result/', folder_name + '_' + 'east' + '.pkl')
    west_pkl_path = os.path.join('./result/', folder_name + '_' + 'west' + '.pkl')
    with open(east_pkl_path, 'wb') as f:
        pickle.dump(east_crop_position_dict, f)
    with open(west_pkl_path, 'wb') as f:
        pickle.dump(west_crop_position_dict, f)






