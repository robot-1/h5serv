##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import json
import argparse
from sets import Set

variable_names = Set()

"""
jsontoh5py - output Python code that generates HDF5 file based on given JSON input  
"""

def getFortranType(hdf5TypeName):
    predefined_int_types = {
          'H5T_STD_I8':  'INTEGER(KIND=1)', 
          'H5T_STD_U8':  '',  # unsigned int types not supported for fortran
          'H5T_STD_I16': 'INTEGER(KIND=2)', 
          'H5T_STD_U16': '',
          'H5T_STD_I32': 'INTEGER(KIND=4)', 
          'H5T_STD_U32': '',
          'H5T_STD_I64': 'INTEGER(KIND=8)',
          'H5T_STD_U64': '' 
    }
    predefined_float_types = {
          'H5T_IEEE_F32': 'REAL(KIND=4)',
          'H5T_IEEE_F64': 'REAL(KIND=8)'
    }
    if len(hdf5TypeName) < 3:
        raise Exception("Type Error: invalid type")
     
    key = hdf5TypeName
    if hdf5TypeName.endswith('LE'):
        key = hdf5TypeName[:-2]
    elif hdf5TypeName.endswith('BE'):
        key = hdf5TypeName[:-2]
     
    fortranType = None   
    if key in predefined_int_types:
        fortranType = predefined_int_types[key]
    if key in predefined_float_types:
        fortranType = predefined_float_types[key]
    
    if not fortranType:
        raise TypeError("Type Error: invalid type")
    return fortranType
    
     

def getBaseDataType(typeItem):
 
    if type(typeItem) == str or type(typeItem) == unicode:
        # should be one of the predefined types
        return getFortranTypename(typeItem)
        code += "np.dtype('" + dtName + "')"
        return code
        
    if type(typeItem) != dict:
        raise TypeError("Type Error: invalid type")
              
    if 'class' not in typeItem:
        raise KeyError("'class' not provided")
    typeClass = typeItem['class']
    shape = ''
    if 'shape' in typeItem:  
        raise TypeError("Array Types are not supported")        
        
    if typeClass == 'H5T_INTEGER':
        if 'base' not in typeItem:
            raise KeyError("'base' not provided")      
        return typeItem['base']
       
    elif typeClass == 'H5T_FLOAT':
        if 'base' not in typeItem:
            raise KeyError("'base' not provided")
        return typeItem['base'] 
        
    elif typeClass == 'H5T_STRING':
        if 'strsize' not in typeItem:
            raise KeyError("'strsize' not provided")
        if 'cset' not in typeItem:
            raise KeyError("'cset' not provided")          
            
        if typeItem['strsize'] == 'H5T_VARIABLE':
            raise TypeError("Variable String types are not supported")
       
        nStrSize = typeItem['strsize']
        if type(nStrSize) != int:
            raise TypeError("expecting integer value for 'strsize'")
        return "S" + str(nStrSize)
        # return "CHARACTER(LEN=" + str(nStrSize) + ")"
            
    elif typeClass == 'H5T_VLEN':
        raise TypeError("VLEN data type is not supported")   
         
    elif typeClass == 'H5T_OPAQUE':
        raise TypeError("Opaque data type is not supported")
    elif typeClass == 'H5T_ARRAY':
        raise TypeError("Array data type is not supported")
    else:
        raise TypeError("Invalid type class")
           
    return code  
    
def valueToString(attr_json):
    value = attr_json["value"]
    return json.dumps(value)

def valueToListString(value):
   # fortran doesn't have a way to initialize multi-dimensional arrays, so
   # flaten array to 1d list
   text = ''
   nelements = len(value)
   line_len = 0
   for i in range(nelements):
       element = value[i]
       if type(element) in (list, tuple):
           next = valueToListString(element) # recursive call
           if i < nelements - 1:
               next += ', '   
           next += ' & \n'  # new line for every sub-array 
       else:
           next = str(element)
           if i < nelements - 1:
               next += ', '
           line_len += len(next)
           if line_len > 75:
               next += ' & \n'
       text += next; 
       
   return text
    
def doAttributeData(attr_json):  
    attr_name = attr_json["name"]
    attr_shape = attr_json["shape"]
    attr_type = attr_json["type"]
    base_type = getBaseDataType(attr_type)
    fortran_type = getFortranType(base_type)
    
    
    print "! attribute " + attr_name + " data declaration" 
    if attr_shape["class"] in ("H5S_NULL", "H5S_SCALAR"):
        print "! no decleration for null, scalar att"
    elif attr_shape["class"] == "H5S_SIMPLE":
        for_array_name = "attr_" + attr_name + "_array"
        dims = attr_shape["dims"]
    	rank = len(dims)
        num_elements = 1
        for i in range(rank):
            num_elements *= dims[i]

        # INTEGER(hsize_t),   DIMENSION(1:2) :: dims = (/dim0, dim1/)
        for_dims = "INTEGER(hsize_t),   DIMENSION(1:" + str(rank) + ") :: " + for_array_name + "_dims = (/"
        for i in range(rank):
            for_dims += str(dims[i])
            if i < rank - 1:
                for_dims += ", "
        for_dims += "/)"
        print for_dims
    	# INTEGER, DIMENSION(1:28), TARGET :: init_array = (/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28/)
    	decl = fortran_type + ", DIMENSION(1:" + str(num_elements) + "), "    
        decl += "TARGET :: attr_" + attr_name + "_array = (/"
        decl += valueToListString(attr_json["value"])
        decl += "/)"
        print decl
        
    else:
	raise typeError("Invalid shape class")

def doAttributeCreate(attr_json, parent_id):  
    attr_name = attr_json["name"]
    attr_shape = attr_json["shape"]
    attr_type = attr_json["type"]
    base_type = getBaseDataType(attr_type)
    fortran_type = getFortranType(base_type)
    
    print "! attribute " + attr_name + " creation" 
    if attr_shape["class"] in ("H5S_NULL", "H5S_SCALAR"):
        print "! no declaration for null, scalar att"
    elif attr_shape["class"] == "H5S_SIMPLE":
        for_array_name = "attr_" + attr_name + "_array"
        dims = attr_shape["dims"]
    
    	rank = len(dims)
    	# INTEGER, DIMENSION(1:28), TARGET :: init_array = (/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28/)
        # f_ptr = C_LOC(init_array(1))
        # call MakeAttribute("myattr", dset, 2, dims, H5T_STD_I64BE, f_ptr, i_res)
        print "f_ptr = C_LOC(" + for_array_name + "(1))"
        print "call CreateAttribute('" + attr_name + "', " + parent_id + ", " + \
		str(rank) + ", " + for_array_name + "_dims, " + base_type + ", f_ptr, i_res)"
        
    else:
	raise typeError("Invalid shape class")
    
def getObjectName(obj_json):
    name = "???"
    if "alias" in obj_json:
        alias = obj_json["alias"]
        if type(alias) in (list, tuple):
            if len(alias) > 0:
                name = alias[0]
        else:
            name = alias
    return name

def doAttributesData(group_json):
    if "attributes" not in group_json:
        return
    attrs_json = group_json["attributes"]
    
    print "! creating attributes for '" + getObjectName(group_json) + "'"
    for attr_json in attrs_json:
        doAttributeData(attr_json)

def doAttributesCreate(group_json, parent_id):
    if "attributes" not in group_json:
        return
    attrs_json = group_json["attributes"]
    
    print "! creating attributes for '" + getObjectName(group_json) + "'"
    for attr_json in attrs_json:
        doAttributeCreate(attr_json, parent_id)
        
def getObjectVariableName(title):
    char_list = list(title.lower())
    for i in range(len(char_list)):
        ch = char_list[i]
        if (ch >= 'a' and ch <= 'z') or (ch >= '0' and ch <= '9'):
            pass  # ok char
        else:
            char_list[i] = '_'  # replace with underscore
    var_name = "".join(char_list)
    if char_list[0] >= '0' and char_list[0] <= '9':
        var_name = 'v' + var_name   # pre-pend with a non-numeric
    if var_name in variable_names:
        for i in range(1, 99):
           if (var_name + '_' + str(i)) not in variable_names:
                var_name += '_' + str(i)
                break
        raise TypeError("Type Error: unable to create type for name: " + title)
    variable_names.add(var_name)  # add to our list of variable names
               
    return var_name
    
def doGroup(h5json, group_id, group_name, parent_var):
    print "# group -- ", group_name
    group_var = getObjectVariableName(group_name)
    print "{0} = {1}.create_group('{2}')".format(group_var, parent_var, group_name)
    groups = h5json["groups"]
    group_json = groups[group_id]
    #print "group_json:", group_json
    doAttributes(group_json, group_var)
    doLinks(h5json, group_json, group_var)

def doDataset(h5json, dset_id, dset_name, parent_var):
    print "# make dataset: ", dset_name
    dset_var = getObjectVariableName(dset_name)
    datasets = h5json["datasets"]
    dset_json = datasets[dset_id]
    #dt = createDataType(dset_json["type"])
    type_json = dset_json["type"]
    dtLine = getBaseDataType(type_json)  # "dt = ..."
    print dtLine
    #dset_type_str = hdf5dtype.getNumpyTypename(type_json["base"])  # todo - read type info, convert to numpy type
    dset_space_str = None
    if "shape" in dset_json:
        shape_json = dset_json["shape"]
        if shape_json["class"] == "H5S_SIMPLE":
            dset_space_str = "("
            dims = shape_json["dims"]
            rank = len(dims)
            for i in range(rank):
                dset_space_str += str(dims[i])
                if i < rank-1 or rank == 1:
                    dset_space_str += ","
            dset_space_str += "), "
    code_line = dset_var + " = " + parent_var + ".create_dataset('" + dset_var + "', "
    if dset_space_str:
        code_line += dset_space_str
    code_line += "dtype=dt)"
    print code_line
    print "# initialize dataset values here"
    doAttributes(dset_json, dset_var)    
    
    
def doLink(h5json, link_json, parent_var):
    if link_json["class"] == "H5L_TYPE_EXTERNAL":
        print "{0}['{1}'] = h5py.ExternalLink('{2}', '{3}')".format(parent_var, link_json["title"], link_json["file"], link_json["h5path"])
    elif link_json["class"] == "H5L_TYPE_SOFT":
        print "{0}['{1}'] = h5py.SoftLink('{2}')".format(parent_var, link_json["title"], link_json["h5path"])
    elif link_json["class"] == "H5L_TYPE_HARD":
        if link_json["collection"] == "groups":
            doGroup(h5json, link_json["id"], link_json["title"], parent_var) 
        elif link_json["collection"] == "datasets":   
            doDataset(h5json, link_json["id"], link_json["title"], parent_var)       
        elif link_json["collection"] == "datatypes":
            pass # todo
        else:
            raise Exception("unexpected collection name: " + link_json["collection"])
    elif link_json["class"] == "H5L_TYPE_UDLINK":
        print "# ignoring user defined link: '{0}'".format(link_json["title"])
    else:
        raise Exception("unexpected link type: " + link_json["class"]) 
    
    
def doLinks(h5json, group_json, parent_var):
    links = group_json["links"]
    for link in links:
        doLink(h5json, link, parent_var)


#------------------------------------------------------------------------------
#
# Generate main fortran routine
#
#------------------------------------------------------------------------------
def fortran_main(h5json, filename): 
    # diction of attribute arrays.
    #  key: array_name
    #  value: array definition
    attr_arrays = {}

    # fortran create root attribute
    root_uuid = h5json["root"]
    group_json = h5json["groups"]
    root_json = group_json[root_uuid]
    

    # fortran main header, file creation
    print """
!-BEGIN_PROLOG-----------------------------------------------------------------
!
! NAME: {0}.f90
!
! PURPOSE: Example program that inserts values into a {0}
!          template file.
!
! REFERENCES: None
!
! COMMENTS: This requires than an empty HDF5 template file has been generated
!           for {0}. This will be the input argument.
!
!           Error checking is not performed for each call. Production code
!           should perform extensive error checking.
!
! TO COMPILE: Rename the generated code to {0}.f90, then
!
!    h5fc h5_param_n_mod.f90 {0}.f90 -o {0}
!
! HISTORY:
!
!   YYYY-MM-DD AUID      SCM  Comment
!   ---------- --------- ---- -------------------------------------------------
!   $dstamp  jelee1         Automatically generated via H5ES Builder
!
!-END_PROLOG-------------------------------------------------------------------
!
program main
!
! Include the HDF5 Library
!
! The HDF5 library is required. The library must be compiled with these flags:
!
!   --enable-fortran --enable-fortran2003
!
! This software has been tested with version 1.8.9.
!
use ISO_C_BINDING
use HDF5
  
implicit none
 
!
! Set error code for a fatal error
!
integer, parameter :: &
    FATAL=3                 ! FATAL errorcode
!
    """.format(filename)
     

    # declare attribute arrays
    doAttributesData(root_json)

    print """
!
! Misc Variables
!
integer :: &
    ios, &                   ! Input/Output status
    i_res                    ! Result code
character(len=255) :: arg    ! Input arguments
integer(HID_T) :: file_id    ! ID of HDF5 file
TYPE(C_PTR) :: f_ptr         ! c ptr for hdf5 lib calls
!
! Open the HDF5 library
!
call H5open_f(i_res)
if (i_res /= 0) then
    print *, 'Error: Initializing HDF library'
    call exit(FATAL)
endif
 
!
! Open HDF5 File
!
call H5Fcreate_f('{0}.h5', H5F_ACC_TRUNC_F, file_id, i_res)
if (i_res /= 0) then
    print *, 'Error: Opening file : {0}.h5'
    call exit(3)
endif

!
! Create the attributes for root group
!
    """.format(filename)
    doAttributesCreate(root_json, "file_id")

    print """
!
! Update the attributes - don't actually overwrite the default attributes,
! just provide sample code to do so.
!
!  call write_attributes(file_id)
!
! Fill the datasets
!
!
! Close the file
!
call H5Fclose_f(file_id, i_res)
!
! Close HDF5
!
call h5close_f(i_res)
!
! End of program
!
print *, ' '
print *, '{0}.f90 complete.'
end program main
    """.format(filename)

   
         
def main():
    
    parser = argparse.ArgumentParser(usage='%(prog)s [-h] <json_file> <out_filename>')
    parser.add_argument('in_filename', nargs='+', help='JSon file to be converted to h5py')
    parser.add_argument('out_filename', nargs='+', help='name of HDF5 file to be created by generated code')
    args = parser.parse_args()
    
    text = open(args.in_filename[0]).read()
    
    # parse the json file
    h5json = json.loads(text)
    
    if "root" not in h5json:
        raise Exception("no root key in input file")

   
    

    filename = args.out_filename[0]
    
    fortran_main(h5json, filename)
  
    
    #group_json = h5json["groups"]
    #root_json = group_json[root_uuid]
    #doAttributes(root_json, file_variable)
    #doLinks(h5json, root_json, file_variable)

    # create attribute routine
    print """
!
! Create an Attribute
!
!
SUBROUTINE CreateAttribute(attr_name, parent_id, rank, dims, h5_type, buf, i_res) 
USE HDF5
USE ISO_C_BINDING
IMPLICIT NONE
  character(len=*), intent(in) :: attr_name
  integer(HID_T), intent(in) :: parent_id
  integer, intent(in) :: rank
  integer(hsize_t), intent(in), dimension(*) :: dims
  integer, intent(in) :: h5_type
  integer, intent(out) :: i_res
  TYPE(C_PTR), intent(in) :: buf
  INTEGER :: hdferr  
  INTEGER(HID_T)  :: space, attr ! Handles
  
  print *, "creat attribute ", attr_name
  print *, "parent id", parent_id
  print *, "rank", rank
  print *, "dims: ", dims(1)
  CALL H5Screate_simple_f(rank, dims, space, hdferr)
  CALL H5Acreate_f(parent_id, attr_name, h5_type, space, attr, hdferr)
  CALL H5Awrite_f(attr, H5T_NATIVE_INTEGER, buf, hdferr)
  CALL H5Aclose_f(attr, hdferr)
  CALL H5Sclose_f(space, hdferr)
  i_res = 0

end SUBROUTINE
    """
    

main()

    
	
