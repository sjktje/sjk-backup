#!/usr/bin/perl


use strict;
use warnings;

use Config::Scoped;
use File::Rsync;
use Parallel::ForkManager;


################################################################################
#### GLOBAL VARIABLES
################################################################################

# Reference to config hash
my $config;

# Verbosity level (log_level in config)
my $verbose;

# Directory containing lock/pid files.
my $lock_directory;


################################################################################
#### SJK-BACKUP
################################################################################

# Read the configuration file.
$config = read_conf();

# Run various checks on config.
check_config();

$verbose = $config->{'general'}{'log_level'};
$lock_directory = strip_trailing_slash($config->{'general'}{'lock_directory'});

# Do the backups.
do_backups($config);


################################################################################
#### FUNCTIONS
################################################################################

# Creates lock file. Prints warning and returns 1 if file already exists.
# If the file doesn't exist it creates it and returns 0.
sub create_lock_file {
	my $name = shift;
	my $file = $lock_directory."/".$name.".lock";
	
	if (-e $file) {
		print_warning("$file does alread exist!", 1);
		return 1;
	}

	open my $fh, ">$file" or die "Could not create $file: $!";
	close $fh;

	return 0;
}

# Write pid to lock file.
sub write_pid {
	my ($file, $pid) = @_;
	
	if (!-e $file) {
		print_warning("$file does not exist.", 1);
		return 1;
	}

	open my $fh, ">$file" or die "Could not open $file for writing: $!";
	print $fh $pid;
	close $fh;

	return 0;
}

# Run checks on config.
sub check_config {
	check_backup_root($config->{'general'}->{'backup_root'});
	check_lock_directory($config->{'general'}->{'lock_directory'});
}

# Returns a reference to a structure holding the config data.
sub read_conf {
	my $parser = new Config::Scoped file => 'sjk-backup.conf';
	$config = $parser->parse;
	return $config;
}

# Check if backup_root is defined, if it exists and is writable.
sub check_backup_root {
	my $root = shift;
	die "No backup_root defined" unless defined $config->{'general'}{'backup_root'};
	die "$root does not exist" unless -e $root;
	die "$root is not writable by us" unless -w $root;
	die "$root is not a directory" unless -d $root;
}

# Check if lock_directory is defined, if it exists and is writable.
sub check_lock_directory {
	my $directory = shift;

	die "No lock_directory defined" unless defined $config->{'general'}{'lock_directory'};
	die "$directory does not exist. Please create it." unless -d $directory;
	die "$directory is not writable" unless -w $directory;
}

# Prints warning if $level is <= $verbose (defined in config as log_level).
sub print_warning {
	my ($message, $level) = @_;
	
	if (!defined($VERBOSE) || ($level <= $VERBOSE)) {
		print STDERR "Warning: $message\n";
	}
}

# Iterate through the hosts that should be backed up and execute the parallel
# rsyncs.
sub do_backups {
	my $config = shift;
	my $pfm = new Parallel::ForkManager(4); # Four rsyncs at a time seems good.
	foreach my $key (keys %{$config->{'backup'}}) {
		# If the lock file already exists a sync is probably already going.
		next if create_lock_file($key); 

		my $pid = $pfm->start and next; # Fork!

		write_pid($key, $pid) or die;

		backup_host($key, $config);
		$pfm->finish;
	}
	$pfm->wait_all_children;
}

sub backup_host {
	my ($name,$conf) = @_;
	my $hostconf = $conf->{'backup'}->{$name};

	my $user = $hostconf->{'user'};
	my $host = $hostconf->{'host'};
	my $backup_root = $conf->{'general'}->{'backup_root'};
	my $bwlimit = $hostconf->{'bwlimit'} ? $hostconf->{'bwlimit'} : 0;

#	print "Time to backup $name...\n";
#	foreach my $key (keys %{$conf}) {
#		print "$key = $conf->{$key}\n";
#	}

	foreach my $path (@{$hostconf->{'path'}}) {

		$path = strip_trailing_slash($path);
		my $src = "$user\@$host:\"$path\"";
		my $dst = "$backup_root/$name.0";

		print "BACKING UP $src TO $dst\n";
		my $rsync = File::Rsync->new({
			'archive'			=> 1,
			'hard-links'		=> 1,
			'human-readable'	=> 1,
			'inplace'			=> 1,
			'numeric-ids'		=> 1,
			'delete'			=> 1,
			'delete-excluded'	=> 1,
			'acls'				=> 1,
			#	'xattr'				=> 1,
			'partial'			=> 1,
			#	'progress'			=> 1,
			'verbose'			=> 1,
			'bwlimit'			=> $bwlimit,
		});
		$rsync->exec({
				src => $src,
				dest => $dst
		}) or warn "Rsync failed.";
	}
}



# Read config file. 
#sub read_conf {
#	open my $fh, '<', 'sjk-backup.conf' or die "Couldn't open config: $!";
#
#	while(<$fh>) {
#		next if is_comment($_);
#		next if is_empty_line($_);
#
#		print "Config: $_";
#
#		if ($_ =~ /^backup_root (.*)$/) {
#			$config{'backup_root'} = $1;
#		}
#
#		if ($_ =~ /^backup ([^@]*)\@(.*):(.*) (.*)$/) {
#			print "Going to backup $1\@$2:$3 to $4\n";
#		}
#	}
#
#	close $fh or die "Couldn't close config: $!";
#
#	die "No backup_root defined." if not defined $config{'backup_root'};
#}

# is_comment($line)
# Returns true if $line is a comment, and false if it is not.
sub is_comment {
	my $line = shift;
	$line =~ s/^(?:\s|\t)+//;
	if ($line =~ /^#/) {
		return 1;
	} else {
		return 0;
	}
}

# is_empty_line($line)
# Returns true if $line is empty, false if it isn't
sub is_empty_line {
	my $line = shift;
	if ($line =~ /^(?:\s|\t)*$/) {
		return 1;
	} else {
		return 0;
	}
}

sub strip_trailing_slash {
	my $line = shift;

	# Don't do anything if $line is a local or remote root filesystem.
	return $line if ($line eq '/');
	return $line if ($line =~ m,:/$,);

	# Otherwise strip trailing slash.
	$line =~ s,/+$,,;

	return $line;
}
