package PVE::Storage::BTRFSPlugin;

use strict;
use warnings;

use File::Path;
use Fcntl qw(O_RDONLY O_WRONLY O_CREAT O_EXCL);

use PVE::Tools qw(run_command);
use PVE::JSONSchema qw(get_standard_option);
use PVE::SafeSyslog;

use PVE::Storage::Plugin;
use base qw(PVE::Storage::Plugin);

# Configuration (same as for DirPlugin)

sub type {
    return 'btrfs';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1, none => 1 },
		     { images => 1, rootdir => 1 } ],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1, subvol => 1 } , 'raw' ],
    };
}

sub properties {
    return {
	# Already defined in DirPlugin
	#path => {
	#    description => "File system path.",
	#    type => 'string', format => 'pve-storage-path',
	#},
    };
}

sub options {
    return {
	path => { fixed => 1 },
	nodes => { optional => 1 },
	shared => { optional => 1 },
	disable => { optional => 1 },
	maxfiles => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
   };
}

# Storage implementation

sub check_config {
    my ($self, $sectionId, $config, $create, $skipSchemaCheck) = @_;
    my $opts = PVE::SectionConfig::check_config($self, $sectionId, $config, $create, $skipSchemaCheck);
    return $opts if !$create;
    if ($opts->{path} !~ m@^/[-/a-zA-Z0-9_.]+$@) {
	die "illegal path for directory storage: $opts->{path}\n";
    }
    return $opts;
}

# croak would not include the caller from within this module
sub __error {
    my ($msg) = @_;
    my (undef, $f, $n) = caller(1);
    die "$msg at $f: $n\n";
}

sub raw_name_to_file($) {
    my ($raw) = @_;
    if ($raw =~ /^(.*)\.raw$/) {
	return "$1/data.raw";
    }
    __error "internal error: bad raw name: $raw";
}

sub raw_file_to_subvol($) {
    my ($file) = @_;
    if ($file =~ m|^(.*)/data\.raw$|) {
	return "$1";
    }
    __error "internal error: bad raw path: $file";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    my $path = $class->get_subdir($scfg, $vtype);

    $path .= "/$vmid" if $vtype eq 'images';

    if ($format eq 'raw') {
	my $file = raw_name_to_file($name);
	if ($snapname) {
	    my $subvol = raw_file_to_subvol($file);
	    $path .= "/$subvol/snap_${snapname}.raw";
	} else {
	    $path .= "/$file";
	}
    } elsif ($snapname && ($format eq 'subvol' || $volname =~ /\.raw$/)) {
	$path .= "/snap_${name}_$snapname";
    } else {
	$path .= "/$name";
    }

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub btrfs_cmd {
    my ($class, $cmd, $outfunc) = @_;

    my $msg = '';
    my $func;
    if (defined($outfunc)) {
	$func = sub {
	    my $part = &$outfunc(@_);
	    $msg .= $part if defined($part);
	};
    } else {
	$func = sub { $msg .= "$_[0]\n" };
    }
    run_command(['btrfs', @$cmd],
	errmsg => 'btrfs error',
	outfunc => $func);

    return $msg;
}

sub btrfs_get_subvol_id {
    my ($class, $path) = @_;
    my $info = $class->btrfs_cmd(['subvolume', 'show', $path]);
    if ($info !~ /^\s*(?:Object|Subvolume) ID:\s*(\d+)$/m) {
	die "failed to get btrfs subvolume ID from: $info\n";
    }
    return $1;
}

sub clone_file {
    my ($src, $dst) = @_;
    #alternatively we could use ioctl($dst_fh, 0x40049409, fileno($src_fh));
    run_command(['cp', '--reflink=always', '--', $src, $dst]);
}

# Other classes have similar function, we explicitly reuse the 'private' one
# from Plugin.pm without exposing it to the $class-> namespace.
my $find_free_diskname = sub {
    return &$PVE::Storage::Plugin::find_free_diskname(@_);
};

# Same as for in the base (Plugin.pm) but takes subvols into account.
# This could use some deduplication
sub create_base {
    my ($class, $storeid, $scfg, $volname, $protect_callback) = @_;
    return PVE::Storage::Plugin::create_base(@_, sub {
	my ($path, $newpath, $format) = @_;
	if ($format eq 'subvol') {
	    rename($path, $newpath) ||
		die "rename '$path' to '$newpath' failed - $!\n";
	    eval { $class->btrfs_cmd(['property', 'set', $newpath, 'ro', 'true']) };
	    warn $@ if $@;
	} elsif ($format eq 'raw') {
	    my $oldvol = raw_file_to_subvol($path);
	    my $newvol = raw_file_to_subvol($newpath);
	    rename($oldvol, $newvol) ||
		die "rename '$oldvol' to '$newvol' failed - $!\n";
	    eval { $class->btrfs_cmd(['property', 'set', $newvol, 'ro', 'true']) };
	} else {
	    rename($path, $newpath) ||
		die "rename '$path' to '$newpath' failed - $!\n";

	    chmod(0444, $newpath); # nobody should write anything

	    # also try to set immutable flag
	    eval { run_command(['/usr/bin/chattr', '+i', $newpath]); };
	    warn $@ if $@;
	}
    });
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    my ($vtype, $basename, $basevmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    my $imagedir = $class->get_subdir($scfg, 'images');
    $imagedir .= "/$vmid";
    mkpath $imagedir;

    if ($format eq 'subvol' || $format eq 'raw' || $snap) {
	my $path = $class->filesystem_path($scfg, $volname, $snap);

	my $name = &$find_free_diskname($imagedir, $vmid, $format);
	warn "clone $volname: $vtype, $name, $vmid to $name (base=../$basevmid/$basename)\n";
	my $newvol = "$basevmid/$basename/$vmid/$name";

	my $newpath = $class->filesystem_path($scfg, $newvol);

	if ($format eq 'subvol') {
	    $class->btrfs_cmd(['subvolume', 'snapshot', '--', $path, $newpath]);
	} elsif ($format eq 'raw') {
	    my $newvol = raw_file_to_subvol($newpath);
	    $class->btrfs_cmd(['subvolume', 'create', '--', $newvol]);
	    eval { clone_file($path, $newpath) };
	    if (my $err = $@) {
		eval { $class->btrfs_cmd(['subvolume', 'delete', '--', $newvol]) };
		warn $@ if $@;
		die $err;
	    }
	} else {
	    die "$format format does not support clone_image from snapshot\n";
	}

	return $newvol;
    }

    return PVE::Storage::Plugin::clone_image(@_);
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    if ($fmt eq 'subvol' || $fmt eq 'raw') {
	my $imagedir = $class->get_subdir($scfg, 'images') . "/$vmid";
	mkpath $imagedir;

	$name = &$find_free_diskname($imagedir, $vmid, $fmt) if !$name;
	my (undef, $tmpfmt) = PVE::Storage::Plugin::parse_name_dir($name);
	die "illegal name '$name' - wrong extension for format ('$tmpfmt != '$fmt')\n"
	    if $tmpfmt ne $fmt;
	my $path;
	if ($fmt eq 'raw') {
	    $path = "$imagedir/" . raw_name_to_file($name);
	} else {
	    $path = "$imagedir/$name";
	}

	die "disk image '$path' already exists\n" if -e $path;

	if ($fmt eq 'subvol') {
	    $class->btrfs_cmd(['subvolume', 'create', '--', $path]);

	    # If we need no limit we're done
	    return "$vmid/$name" if !$size;

	    # Use the subvol's default 0/$id qgroup
	    eval {
		$class->btrfs_cmd(['quota', 'enable', $path]);
		my $id = $class->btrfs_get_subvol_id($path);
		$class->btrfs_cmd(['qgroup', 'limit', "${size}k", "0/$id", $path]);
	    };
	    if (my $err = $@) {
		$class->btrfs_cmd(['subvolume', 'delete', '--', $path]);
		die $err;
	    }

	    return "$vmid/$name";
	} else { # raw
	    my $vol = raw_file_to_subvol($path);
	    $class->btrfs_cmd(['subvolume', 'create', '--', $vol]);
	    eval {
		run_command(['/usr/bin/qemu-img', 'create',
		    '-f', $fmt, $path, "${size}K"],
		    errmsg => "unable to create image");
	    };
	    if (my $err = $@) {
		eval { $class->btrfs_cmd(['subvolume', 'delete', '--', $vol]) };
		warn $@ if $@;
		die $err;
	    }
	    return "$vmid/$name";
	}
    }

    return PVE::Storage::Plugin::alloc_image(@_);
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    my $path = $class->filesystem_path($scfg, $volname);

    if (defined($format) && $format eq 'subvol') {
	$class->btrfs_cmd(['subvolume', 'delete', $path]);
	return undef;
    }
    elsif (defined($format) && $format eq 'raw') {
	my $vol = raw_file_to_subvol($path);
	$class->btrfs_cmd(['subvolume', 'delete', $vol]);
	return undef;
    }

    return PVE::Storage::Plugin::free_image(@_);
}

sub btrfs_subvol_quota {
    my ($class, $path) = @_;
    my $id = '0/' . $class->btrfs_get_subvol_id($path);
    my $search = qr/^\Q$id\E\s+\d+\s+\d+\s+(\d+)\s*$/;
    my $size;
    $class->btrfs_cmd(['qgroup', 'show', '--raw', '-rf', '--', $path], sub {
	if (!defined($size) && $_[0] =~ $search) {
	    $size = $1;
	}
    });
    if (!defined($size)) {
	# syslog should include more information:
	syslog('err', "failed to get subvolume size for: $path (id $id)");
	# UI should only see the last path component:
	$path =~ s|^.*/||;
	die "failed to get subvolume size for $path\n";
    }
    return $size;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my $path = $class->filesystem_path($scfg, $volname);

    my $format = ($class->parse_volname($volname))[6];

    if ($format eq 'subvol') {
	return $class->btrfs_subvol_quota($path);
    }

    return PVE::Storage::Plugin::file_size_info($path, $timeout);
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my $format = ($class->parse_volname($volname))[6];
    if ($format eq 'subvol') {
	my $path = $class->filesystem_path($scfg, $volname);
	my $id = '0/' . $class->btrfs_get_subvol_id($path);
	$class->btrfs_cmd(['qgroup', 'limit', "${size}k", $id, $path]);
	return undef;
    }

    return PVE::Storage::Plugin::volume_resize(@_);
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($name, $format) = ($class->parse_volname($volname))[1,6];
    if ($format eq 'subvol' || $format eq 'raw') {
	my $path = $class->filesystem_path($scfg, $volname);
	my $snap_path = $class->filesystem_path($scfg, $volname, $snap);

	if ($format eq 'subvol') {
	    $class->btrfs_cmd(['subvolume', 'snapshot', '--', $path, $snap_path]);
	} else { #raw
	    clone_file($path, $snap_path);
	}
	return undef;
    }

    return PVE::Storage::Plugin::volume_snapshot(@_);
}

sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    return 1;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($name, $format) = ($class->parse_volname($volname))[1,6];
    if ($format eq 'subvol' || $volname =~ /\.raw$/) {
	my $path = $class->filesystem_path($scfg, $volname);
	my $snap_path = $class->filesystem_path($scfg, $volname, $snap);
	if ($format eq 'subvol') {
	    # FIXME: use RENAME_EXCHANGE once the kernel supports it on btrfs.
	    rename($path, "$path.tmp") or die "failed to rename subvol: $!\n";
	    eval { $class->btrfs_cmd(['subvolume', 'snapshot', '--', $snap_path, "$path"]) };
	    if (my $err = $@) {
		rename("$path.tmp", $path) or die "failed to restore subvolume after error: $!\n";
		die $err;
	    }
	    eval { $class->btrfs_cmd(['subvolume', 'delete', '--', "$path.tmp"]) };
	    warn $@ if $@;
	} else { # raw
	    clone_file($snap_path, $path);
	}
	return undef;
    }

    return PVE::Storage::Plugin::volume_snapshot_rollback(@_);
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($name, $format) = ($class->parse_volname($volname))[1,6];
    if ($format eq 'subvol' || $volname =~ /\.raw$/) {
	my $snap_path = $class->filesystem_path($scfg, $volname, $snap);
	if ($format eq 'subvol') {
	    $class->btrfs_cmd(['subvolume', 'delete', '--', $snap_path]);
	} else { # raw
	    unlink($snap_path)
		or die "failed to unlink snapshot $snap_path: $!\n";
	}
	return undef;
    }

    return PVE::Storage::Plugin::volume_snapshot_delete(@_);
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => { current => 1, snap => 1 },
	clone => { base => 1 },
	template => { current => 1 },
	copy => { base => 1, current => 1, snap => 1 },
	sparseinit => { base => 1, current => 1 },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
	$class->parse_volname($volname);

    my $key = undef;
    if($snapname){
        $key = 'snap';
    }else{
        $key =  $isBase ? 'base' : 'current';
    }

    return 1 if defined($features->{$feature}->{$key});

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;
    my $imagedir = $class->get_subdir($scfg, 'images');

    my $res = [];

    foreach my $fn (<$imagedir/[0-9][0-9]*/*>) {
	next if $fn !~ m@^(/.+/(\d+)/([^/.]+)(?:\.(qcow2|vmdk|subvol))?)$@;
	$fn = $1; # untaint

	my $owner = $2;
	my $name = $3;
	my $ext = $4;

	next if !$vollist && defined($vmid) && ($owner ne $vmid);

	my $volid = "$storeid:$owner/$name";
	$volid .= ".$ext" if defined($ext);
	my ($size, $format, $used, $parent);
	if (!$ext) { # raw
	    $volid = "$storeid:$owner/$name.raw",

	    my $rawfile = "$fn/data.raw";
	    ($size, $format, $used, $parent) = PVE::Storage::Plugin::file_size_info($rawfile);
	} elsif ($ext eq 'subvol') { # subvolume
	    $size = $class->btrfs_subvol_quota($fn);
	    $format = 'subvol';
	    $used = 0; # FIXME
	} else {
	    ($size, $format, $used, $parent) = PVE::Storage::Plugin::file_size_info($fn);
	    next if !($format && defined($size));
	}

	if ($vollist) {
	    next if ! grep { $_ eq $volid } @$vollist;
	}

	push @$res, {
	    volid => $volid, format => $format,
	    size => $size, vmid => $owner, used => $used, parent => $parent,
	};
    }

    return $res;
}

1;
