#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from unrar import rarfile
from unrar.unrarlib import MissingPassword, UnrarException
from hurry.filesize import size
#import patoolib, pyunpack
from pathlib import Path
from pprint import pprint
import argparse
import colorlog
import logging
import shutil
import time
import re


def get_rar_list_from_dir(path=".", g="*.rar"):
    if isinstance(path,Path):
        p = path
    elif isinstance(path,str):
        p = Path(path)
    files = sorted((path.glob('*.rar')))
    return files


def filterMultiPart(path_list):
    regex = r"^(?!.*part0*(?:[2-9]\d*|[1-9]\d+)\.rar)(.+)\.rar$"
    r = re.compile(regex, re.MULTILINE)
    return list(filter(lambda x: r.match(x.name), path_list))


def getAllParts(path_list, path_obj):
    TEXTO = re.compile(r"^(.*?)(?:part\d*)?\.rar$", re.MULTILINE).match(path_obj.name).groups()[0]
    pattern = rf"({re.escape(TEXTO)})(?:part\d*)?\.rar"
    logger.debug("regex", pattern)
    r = re.compile(pattern)
    return list(filter(lambda x: r.match(x.name), path_list))


def get_RarFile(path_obj):
    p = str(path_obj.absolute())
    if rarfile.is_rarfile(p):
        try:
            return rarfile.RarFile(p)
        except (MissingPassword, RuntimeError, UnrarException) as e:
            logger.exception(f"RuntimeError {type(e)} {str(e)} {p}")
        except Exception as e:
            logger.exception(f"Exception {type(e)} {str(e)} {p}")
    logger.error(f"Not a RAR file {path_obj.absolute()}")


def checkRAR(rarf, path):
    try:
        logger.info(f"Trying  testrar() {rarf.filename}")
        ko = rarf.testrar()
        if ko is not None:
            logger.error(f"Problem testrar() {rarf.filename} ==> {str(ko)}")
            return ko
    except (MissingPassword, RuntimeError, UnrarException) as e:
        logger.exception(f"{type(e)} {str(e)} {rarf.filename}", exc_info=True)


def extract_rar(rarfilz, path_obj=Path(".")):
    extract_dir = path_obj
    try:
        logger.info(f"Trying  extractall() {rarfilz.filename}")
        ret = rarfilz.extractall(path=str(extract_dir.absolute()))
        # pyunpack.Archive(archive_file).extractall(extract_dir)
        if ret is None:
            logger.info(f"extractall OK: {rarfilz.filename}")
        else:
            logger.info(f"extractall KO: {ret}")
    except (MissingPassword, RuntimeError, UnrarException) as e:
        logger.exception(f"MissingPassword {type(e)} {str(e)} {rarfilz.filename}", exc_info=True)


def get_Archive_Compress_Size(rarfilz):
    return sum([r.compress_size for r in rarfilz.infolist()])


def get_Archive_Uncompress_Size(rarfilz):
    return sum([r.file_size for r in rarfilz.infolist()])


def get_rootfolder(rarfilz):
    rootfolder = rarfilz.namelist()[-1]
    return rootfolder


def get_Available_space(path_obj):
    path_obj.mkdir(exist_ok=True)
    return shutil.disk_usage(path_obj.absolute()).free


def is_enough_space_available(rarfilz, path_obj):
    if get_Archive_Uncompress_Size(rarfilz) < get_Available_space(path_obj):
        return True


def mv(path_obj_src, path_obj_dst):
    # shutil.rmtree
    path_s = str(path_obj_src.absolute())
    path_d = path_obj_dst
    try:
        ret = shutil.move(path_s, path_d)
        tmp = str(Path(ret).absolute())
        shouldbe = str(path_d.joinpath(path_obj_src.name).absolute())
        if tmp == shouldbe:
            return True
        logger.error(f"{ret} {path_s} to {path_d}")
        return False
    except shutil.Error as e:
        logger.exception(f"Exception {type(e)} {str(e)} {path_s}")


def mv_list(path_obj_list, path_obj_dst):
    lst = list(map(lambda x: mv(x, path_obj_dst), path_obj_list))
    if not all(lst):
        logger.error(f"{path_obj_list} to {path_obj_dst}")
    return lst


def main(crt_dir, wrk_dir="_peon/"):
    PATH = crt_dir
    working_path = Path(crt_dir).joinpath(wrk_dir)
    dic = {"success": [], "error": []}

    path_list = get_rar_list_from_dir(crt_dir, "*.rar")
    path_files = filterMultiPart(path_list)
    logger.info(f"{path_files}")
    for path_elem in path_files:
        rarf = get_RarFile(path_elem)
        if rarf is not None:
            ok = checkRAR(rarf, path_elem)
            if ok is not None:
                dic['error'].append(path_elem)
                continue
            time.sleep(5)
            logger.info(f"rootfolder?={get_rootfolder(rarf)} Uncompress={size(get_Archive_Uncompress_Size(rarf))}")
            if is_enough_space_available(rarf, working_path):
                extract_rar(rarf, working_path)
                dic['success'].append(path_elem)
                rar_parts = getAllParts(path_list, path_elem)
                logger.info(f"Files to move: {rar_parts}")
                time.sleep(5)
                mv_list(rar_parts, working_path)
            else:
                logger.error(f"Not enough space available {path_elem.absolute()}")
        else:
            logger.error(f"RAR file {path_elem.absolute()}")
        time.sleep(10)
    return dic


if __name__ == "__main__":
    # TODO multipart  get list of multipart
    # TODO: check if  only one root folder present inside archive
    # TODO: find *.rar in target directory
    # TODO: multipart ismisssing (verify testrar)

    # TODO: all extracted size of archive_file
    # TODO: verify uncompress size of archive is avaible on target volume
    # TODO: remove all rar if
    # TODO: match scene group

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
    	'%(log_color)s[%(asctime)s] [%(levelname)s] [%(funcName)s_l%(lineno)-3s]\t\t%(message)s'
        #'%(log_color)[%(asctime)s] [%(levelname)s]  [%(funcName)s_(l%(lineno)-3s\t%(message)s'
        ))

    logger = colorlog.getLogger('__name__')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-i',"--input", type=Path, action="store", nargs='?', help="input path",  default=".")
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()


    dic = main(args.input)
    pprint(dic)
