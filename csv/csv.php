<?php

    if ( !defined('K_COUCH_DIR') ) die(); // cannot be loaded directly

    class KCSV_Reader{

        protected $filename;
        protected $delimiter;
        protected $enclosure;
        protected $has_header;
        protected $use_cache;

        protected $fp;
        protected $cols = array();

        protected $offsets = array();
        protected $offset_interval = 100;
        protected $offset_first_row = 0;

        protected $row_count = 0;
        var $error;


        function __construct( $filename, $has_header=1, $use_cache=0, $delimiter=',', $enclosure='"', $escape='"' ){
            global $FUNCS;

            $this->filename = $filename;
            $this->delimiter = $delimiter;
            $this->enclosure = $enclosure;
            $this->escape = $escape;
            $this->has_header = $has_header;
            $this->use_cache = $use_cache;


            ini_set( "auto_detect_line_endings", true );

            if( !file_exists($this->filename) ){
                $this->error = $FUNCS->raise_error( "File ".$this->filename." not found" );
                return;
            }

            if( !($this->fp = fopen($this->filename, "rb")) ){
                $this->error = $FUNCS->raise_error( "Failed to open file ".$this->filename );
                return;
            }

            // UTF BOM present?
            $bombytes = fread( $this->fp, 3 );
            if( $bombytes != chr(0xEF) . chr(0xBB) . chr(0xBF) ){
                fseek( $this->fp, 0, SEEK_SET ); // rewind
            }

            // get row count and row offsets
            $this->_fill_stats();
        }

        // Returns a non-empty row from the current file pointer position.
        // Returns false if no row could be read.
        function get_next(){

            $row = array();

            while( 1 ){
                $row = fgetcsv( $this->fp, 0, $this->delimiter, $this->enclosure, $this->escape );
                if( $row==false || is_null($row) ){ // EOF or invalid file handle
                    return false;
                }
                else{
                    // empty? A blank line in a CSV file will be returned as an array comprising a single null field
                    if( count($row)==1 && empty($row[0]) ) continue;
                }
                break;
            }

            return $row;
        }

        function get_rows( $offset, $limit, $with_keys=0 ){

            $rows = array();

            $rc = $this->_skip_rows( $offset );
            if( !$rc ) return $rows;

            $col_count = count( $this->cols );

            for( $x=0; $x<$limit; $x++ ){
                $row = $this->get_next();
                if( $row==false ) break;

                if( $with_keys ){
                    $element_count = count( $row );
                    $cols = $this->cols;

                    if( $element_count != $col_count ){
                        if( $col_count > $element_count ){
                            $cols = array_slice( $cols, 0, $element_count );
                        }
                        else{
                            $row = array_slice( $row, 0, $col_count );
                        }
                    }

                    $row = array_combine( $cols, $row );
                }

                $rows[] = $row;
            }

            return $rows;
        }

        protected function _skip_rows( $offset ){

            // calculate file offset to place pointer on
            $file_offset = floor( $offset/$this->offset_interval );
            $file_offset *= $this->offset_interval;
            if( !isset($this->offsets[$file_offset]) ){
                return 0;
            }
            fseek( $this->fp, $this->offsets[$file_offset], SEEK_SET );

            for( $x=$file_offset; $x<$offset; $x++ ){
                $row = $this->get_next();
                if( $row==false ) break;
            }

            return 1;

        }

        protected function _fill_stats(){
            global $FUNCS;

            $filepath = $this->filename;
            if( !file_exists($filepath) ) die( "Module CSV: File ".$filepath." not found" );

            if( $this->use_cache ){
                $last_mod = @filemtime( $filepath );
                $cache_key = 'csv_cache_' . md5( $filepath );
                $cache_value = @unserialize( base64_decode($FUNCS->get_setting($cache_key)) );
            }

            if( (!$this->use_cache) || (!is_array($cache_value)) || ($last_mod > $cache_value['last_mod']) || ($this->has_header != $cache_value['has_header']) ){

                // calculate stats
                if( $this->has_header ){
                    $row = $this->get_next();
                    $this->cols = ( $row!=false ) ? array_map( "trim", $row ) : array();
                }

                $this->offset_first_row = ftell( $this->fp );

                ini_set( 'max_execution_time', 120 );
                $count = 0;
                while( ($row = $this->get_next())!=false ){
                    $count++;

                    if( $count==1 && !$this->has_header ){
                        $this->cols = array();
                        for( $x=0; $x<count($row); $x++ ){
                            $this->cols[$x] = 'col_'.($x+1);
                        }
                    }

                    if( !($count % $this->offset_interval) ){
                        $this->offsets[$count] = ftell( $this->fp );
                    }
                }
                if( $count ){
                    $this->offsets[0] = $this->offset_first_row;
                }
                $this->row_count = $count;

                // cache stats
                if( $this->use_cache ){
                    $cache_value =  base64_encode( serialize(array(
                                        'last_mod'=>$last_mod,
                                        'has_header'=>$this->has_header,
                                        'offsets'=>$this->offsets,
                                        'offset_first_row'=>$this->offset_first_row,
                                        'row_count'=>$this->row_count,
                                        'cols'=>$this->cols
                                        )) );
                    $FUNCS->set_setting( $cache_key, $cache_value );
                }
            }
            else{
                $this->offset_first_row = $cache_value['offset_first_row'];
                $this->offsets = $cache_value['offsets'];
                $this->row_count = $cache_value['row_count'];
                $this->cols = $cache_value['cols'];
            }

            // rewind
            fseek( $this->fp, $this->offset_first_row, SEEK_SET );
        }

        function row_count(){
            return $this->row_count;
        }

        function header_count(){
            return count( $this->cols );
        }

        ////////////////////////////////// Tags ////////////////////////////////
        static function csv_reader_handler( $params, $node ){
            global $FUNCS, $CTX, $PAGE;

            extract( $FUNCS->get_named_vars(
                array(
                    'file'=>'',
                    'has_header'=>'1',
                    'use_cache'=>'0',
                    'delimiter'=>'',
                    'enclosure'=>'',
                    'escape'=>'', /* escape char being used by the csv.. default is '\' to match the default of PHP */

                    'limit'=>'',
                    'offset'=>'0',
                    'startcount'=>'',
                    'paginate'=>'0', /*ignores $_GET['pg'] if 0*/
                    'qs_param'=>'', /* custom var in querystring that denotes paginated page */
                    'base_link'=>'', /* replaces the default $PAGE->link used for paginator crumb links */
                    'count_only'=>'0',
                    'prefix'=>'', /* gets prefixed to field names */
                    'locale'=>'',
                ),
                $params)
            );

            // sanitize params
            $file = trim( $file );
            if( !$file ){ die("ERROR: Tag \"".$node->name."\" requires a 'file' parameter"); }
            $has_header = ( $has_header==0 ) ? 0 : 1;
            $use_cache = ( $use_cache==1 ) ? 1 : 0;

            $delimiter = trim( $delimiter );
            if( !strlen($delimiter) ){ $delimiter=','; }
            elseif( strlen($delimiter)>1 ){
                if( $delimiter == '\r\n' ){ $delimiter = "\n"; }
                elseif( $delimiter == '\r' ){ $delimiter = "\r"; }
                elseif( $delimiter == '\n' ){ $delimiter = "\n"; }
                elseif( $delimiter == '\t' ){ $delimiter = "\t"; }
                else{ die("ERROR: Tag \"".$node->name."\": 'delimiter' must be a single character"); }
            }

            $enclosure = trim( $enclosure );
            if( !strlen($enclosure) ){ $enclosure='"'; }
            elseif( strlen($enclosure)>1 ){
                die("ERROR: Tag \"".$node->name."\": 'enclosure' must be a single character");
            }

            $escape = trim( $escape );
            if( !strlen($escape) ){ $escape=$enclosure; } // RFC-4180 compliance
            elseif( strlen($escape)>1 ){
                die("ERROR: Tag \"".$node->name."\": 'escape' must be a single character");
            }

            $limit = $FUNCS->is_non_zero_natural( $limit ) ? intval( $limit ) : 1000;
            $offset = $FUNCS->is_natural( $offset ) ? intval( $offset ) : 0;
            $startcount = $FUNCS->is_int( $startcount ) ? intval( $startcount ) : 1;
            $paginate = ( $paginate==1 ) ? 1 : 0;
            $qs_param = trim( $qs_param );
            if( $qs_param=='' ){  $qs_param = 'pg'; }
            $base_link = trim( $base_link );
            $count_only = ( $count_only==1 ) ? 1 : 0;
            $prefix = trim( $prefix );
            $locale = trim( $locale );

            $pgn_pno = 1;
            if( $paginate ){
                if( isset($_GET[$qs_param]) && $FUNCS->is_non_zero_natural( $_GET[$qs_param] ) ){
                    $pgn_pno = (int)$_GET[$qs_param];
                }
            }

            if( $locale ){
                $orig_locale = setlocale( LC_ALL, "0" );
                @setlocale( LC_ALL, $locale );
            }

            // get down to business ..
            $csv = new KCSV_Reader( $file, $has_header, $use_cache, $delimiter, $enclosure, $escape );
            if( $FUNCS->is_error($csv->error) ){ die("ERROR: Tag \"".$node->name."\": " . $csv->error->err_msg); }

            $total_rows = $csv->row_count();

            // Return if only count asked for
            if( $count_only ) return $total_rows;

            // get rows
            $skip = (($pgn_pno - 1) * $limit) + $offset;
            $rows = $csv->get_rows( $skip, $limit, 1 );
            $count = count( $rows );

            $total_rows -= $offset;
            $total_pages = ceil( $total_rows/$limit );

            $page_link = ( strlen($base_link) ) ?  $base_link : K_SITE_URL . $PAGE->link;

            // append querystring params, if any
            $sep = '';
            // HOOK: skip_qs_params_in_paginator
            $skip_qs = array();
            $FUNCS->dispatch_event( 'skip_qs_params_in_paginator', array(&$skip_qs) );
            foreach( $_GET as $qk=>$qv ){
                if( $qk=='p' || $qk=='f' || $qk=='d' || $qk=='fname'|| $qk=='pname' || $qk=='_nr_' ) continue;
                if( $qk==$qs_param ) continue;
                if( in_array($qk, $skip_qs) ) continue;

                if( is_array($qv) ){ //checkboxes
                    foreach( $qv as $qvv ){
                        $qs .= $sep . $qk . '[]=' . urlencode($qvv);
                        $sep = '&';
                    }
                }
                else{
                    $qs .= $sep . $qk . '=' . urlencode($qv);
                    $sep = '&';
                }
            }

            if( $qs ){
                $page_link .= ( strpos($page_link, '?')===false ) ? '?' : '&';
                $page_link .= $qs;
            }

            if( $total_rows > $limit ){
                $paginated = 1;
                $sep = ( strpos($page_link, '?')===false ) ? '?' : '&';

                // 'Prev' link
                if( $pgn_pno > 1 ){
                    if( $pgn_pno==2 ){
                        $pgn_prev_link = $page_link;
                    }
                    else{
                        $pgn_prev_link = sprintf( "%s%s%s=%d", $page_link, $sep, $qs_param, $pgn_pno-1 );
                    }
                }
                // 'Next' link
                if( $pgn_pno < $total_pages ){
                    $pgn_next_link = sprintf( "%s%s%s=%d", $page_link, $sep, $qs_param, $pgn_pno+1 );
                }

                // Current paginated link
                $pgn_cur_link = ( $pgn_pno==1 ) ? $page_link : sprintf( "%s%s%s=%d", $page_link, $sep, $qs_param, $pgn_pno );
            }

            if( $count ){
                for( $x=0; $x<$count; $x++ ){

                    $rec = $rows[$x];

                    // set headers as array to be iterated by cms:csv_headers tag
                    $CTX->set_object( 'headers', $csv->cols );

                    // set column values as discrete variables
                    $CTX->reset();
                    $CTX->set( 'k_csv_header_count', $csv->header_count() );
                    $CTX->set( 'k_csv_column_count', count($rec) );
                    foreach( $rec as $k=>$v ){
                        $CTX->set( $prefix.$k, $v );
                    }

                    // set column values as array to be iterated by cms:csv_columns tag
                    $CTX->set_object( 'columns', $rec );

                    // Pagination related variables
                    $first_record_on_page = ($limit * ($pgn_pno - 1)) + $startcount;
                    $total_records_on_page = ( $count<$limit ) ? $count : $limit;
                    $CTX->set( 'k_count', $x + $startcount );
                    $CTX->set( 'k_total_records', $total_rows );
                    $CTX->set( 'k_total_records_for_pagination', $total_rows );
                    $CTX->set( 'k_total_records_on_page', $total_records_on_page );
                    $CTX->set( 'k_current_record', $first_record_on_page + $x );
                    $CTX->set( 'k_absolute_count', $first_record_on_page + $x ); //same as current record
                    $CTX->set( 'k_record_from', $first_record_on_page );
                    $CTX->set( 'k_record_to', $first_record_on_page + $total_records_on_page - 1 );
                    $CTX->set( 'k_total_pages', $total_pages );
                    $CTX->set( 'k_current_page', $pgn_pno );
                    $CTX->set( 'k_paginate_limit', $limit );


                    if( $x==0 ){
                        $CTX->set( 'k_paginated_top', 1 );
                    }
                    else{
                        $CTX->set( 'k_paginated_top', 0 );
                    }

                    if( $x==$count-1 ){
                        $CTX->set( 'k_paginated_bottom', 1 );
                    }
                    else{
                        $CTX->set( 'k_paginated_bottom', 0 );
                    }

                    if( /*($x==0 || $x==$count-1) &&*/ $paginate && $paginated ){
                        $CTX->set( 'k_paginator_required', 1 );
                        $CTX->set( 'k_page_being_paginated', $page_link );
                        $CTX->set( 'k_qs_param', $qs_param );
                        $CTX->set( 'k_paginate_link_next', $pgn_next_link );
                        $CTX->set( 'k_paginate_link_prev', $pgn_prev_link );
                        $CTX->set( 'k_paginate_link_cur', $pgn_cur_link );
                    }
                    else{
                        $CTX->set( 'k_paginator_required', 0 );
                        $CTX->set( 'k_paginate_link_next', '' );
                        $CTX->set( 'k_paginate_link_prev', '' );
                        $CTX->set( 'k_paginate_link_cur', '' );
                    }

                    // call the children
                    foreach( $node->children as $child ){
                        $html .= $child->get_HTML();
                    }
                }
            }
            else{ // find and execute 'no_results' tag
                $html = '';
                foreach( $node->children as $child ){
                    if( $child->type == K_NODE_TYPE_CODE && $child->name == 'no_results' ){
                        // call the children of no_results
                        foreach( $child->children as $grand_child ){
                            $html .= $grand_child->get_HTML();
                        }
                        break;
                    }
                }
            }

            if( $locale ){
                @setlocale( LC_ALL, $orig_locale );
            }

            return $html;
        }

        static function csv_headers_handler( $params, $node ){
            global $FUNCS, $CTX;

            extract( $FUNCS->get_named_vars(
                array(
                    'startcount'=>'',
                ),
                $params)
            );

            // sanitize params
            $startcount = $FUNCS->is_int( $startcount ) ? intval( $startcount ) : 1;

            $headers = &$CTX->get_object( 'headers', 'csv_reader' );
            if( is_array($headers) ){
                $total = count( $headers );
                for( $x=0; $x<$total; $x++ ){
                    $CTX->set( 'k_count', $x + $startcount );
                    $CTX->set( 'k_total_records', $total );
                    $CTX->set( 'value', $headers[$x] );

                    // call the children
                    foreach( $node->children as $child ){
                        $html .= $child->get_HTML();
                    }

                }
            }

            return $html;
        }

        static function csv_columns_handler( $params, $node ){
            global $FUNCS, $CTX;

            extract( $FUNCS->get_named_vars(
                array(
                    'startcount'=>'',
                ),
                $params)
            );

            // sanitize params
            $startcount = $FUNCS->is_int( $startcount ) ? intval( $startcount ) : 1;

            $columns = &$CTX->get_object( 'columns', 'csv_reader' );
            if( is_array($columns) ){
                $x = 0;
                $total = count( $columns );
                foreach( $columns as $k=>$v ){
                    $CTX->set( 'k_count', $x + $startcount );
                    $CTX->set( 'k_total_records', $total );
                    $CTX->set( 'key', $k );
                    $CTX->set( 'value', $v );

                    // call the children
                    foreach( $node->children as $child ){
                        $html .= $child->get_HTML();
                    }

                    $x++;
                }
            }

            return $html;
        }

    }

    $FUNCS->register_tag( 'csv_reader', array('KCSV_Reader', 'csv_reader_handler'), 1, 1 );
    $FUNCS->register_tag( 'csv_headers', array('KCSV_Reader', 'csv_headers_handler'), 1, 1 );
    $FUNCS->register_tag( 'csv_columns', array('KCSV_Reader', 'csv_columns_handler'), 1, 1 );
